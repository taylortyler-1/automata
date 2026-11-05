package telegram_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/taylortyler-1/automata/core"
	"github.com/taylortyler-1/automata/extensions/messaging"
	"github.com/taylortyler-1/automata/extensions/messaging/telegram"
)

func init() { slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) }

// fakeAPI is a minimal Bot API server.
type fakeAPI struct {
	mu        sync.Mutex
	calls     []apiCall
	nextID    int64
	updates   chan json.RawMessage
	queued    []json.RawMessage // updates kept until confirmed by offset
	offsets   []int64
	failToast bool
	messages  map[int64]string // message ID -> current text
	markup    map[int64]bool   // message ID -> has inline keyboard
	answers   []string
}

type apiCall struct {
	Method string
	Body   map[string]any
}

func newFakeAPI(t *testing.T) (*fakeAPI, *httptest.Server) {
	f := &fakeAPI{nextID: 100, updates: make(chan json.RawMessage, 10), messages: map[int64]string{}, markup: map[int64]bool{}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeAPI) serve(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	method := parts[len(parts)-1]
	if parts[1] != "botTEST" {
		w.Write([]byte(`{"ok":false,"error_code":401,"description":"Unauthorized"}`))
		return
	}
	var body map[string]any
	json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.calls = append(f.calls, apiCall{method, body})
	f.mu.Unlock()
	reply := func(result any) {
		raw, _ := json.Marshal(map[string]any{"ok": true, "result": result})
		w.Write(raw)
	}
	switch method {
	case "sendMessage":
		f.mu.Lock()
		f.nextID++
		id := f.nextID
		f.messages[id] = body["text"].(string)
		markup, _ := body["reply_markup"].(map[string]any)
		_, f.markup[id] = markup["inline_keyboard"]
		f.mu.Unlock()
		reply(map[string]any{"message_id": id, "chat": map[string]any{"id": body["chat_id"]}})
	case "editMessageText":
		id := int64(body["message_id"].(float64))
		f.mu.Lock()
		f.messages[id] = body["text"].(string)
		_, f.markup[id] = body["reply_markup"]
		f.mu.Unlock()
		reply(true)
	case "answerCallbackQuery":
		f.mu.Lock()
		f.answers = append(f.answers, fmt.Sprint(body["text"]))
		fail := f.failToast
		f.mu.Unlock()
		if fail {
			w.Write([]byte(`{"ok":false,"error_code":400,"description":"query is too old"}`))
			return
		}
		reply(true)
	case "deleteWebhook":
		reply(true)
	case "getUpdates":
		// Like Telegram: updates stay queued until a later call's offset
		// confirms them.
		offset := int64(0)
		if v, ok := body["offset"].(float64); ok {
			offset = int64(v)
		}
		deadline := time.After(200 * time.Millisecond)
		for {
			f.mu.Lock()
			f.offsets = append(f.offsets, offset)
		drain:
			for {
				select {
				case u := <-f.updates:
					f.queued = append(f.queued, u)
				default:
					break drain
				}
			}
			var pending []json.RawMessage
			for _, u := range f.queued {
				var head struct {
					UpdateID int64 `json:"update_id"`
				}
				json.Unmarshal(u, &head)
				if head.UpdateID >= offset {
					pending = append(pending, u)
				}
			}
			f.mu.Unlock()
			if len(pending) > 0 {
				reply(pending)
				return
			}
			select {
			case <-deadline:
				reply([]any{})
				return
			case <-r.Context().Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	default:
		w.Write([]byte(`{"ok":false,"error_code":404,"description":"Not Found"}`))
	}
}

func (f *fakeAPI) text(id int64) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.messages[id], f.markup[id]
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type scripted struct {
	mu      sync.Mutex
	replies []core.Message
}

func (p *scripted) Invoke(context.Context, core.Request) (core.Response, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.replies) == 0 {
		return core.Response{}, errors.New("script exhausted")
	}
	reply := p.replies[0]
	p.replies = p.replies[1:]
	stop := core.StopEndTurn
	if len(reply.ToolUses()) > 0 {
		stop = core.StopToolUse
	}
	return core.Response{Message: reply, StopReason: stop}, nil
}

// TestApproveFromTelegramEndToEnd wires runtime, router and bot against the
// fake API: the notice is sent with buttons, a callback from the owner
// approves it, the tool runs, and the notice is edited without buttons.
func TestApproveFromTelegramEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api, srv := newFakeAPI(t)
	bot, err := telegram.New(telegram.Config{Token: "TEST", ChatID: 42, BaseURL: srv.URL, WebhookSecret: "s3cret"})
	if err != nil {
		t.Fatal(err)
	}
	approvers := messaging.Approvers{"telegram": {"42"}}
	store := core.NewMemoryStore()
	rt, err := core.NewRuntime(ctx, core.RuntimeConfig{Store: store, Authorizer: approvers.Authorizer(nil)})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	published := 0
	tool := core.WithDurableWait(
		core.Func("open_pr", "Open a PR with a post.", func(context.Context, struct {
			Title string `json:"title"`
		}) (string, error) {
			published++
			return "PR #7", nil
		}),
		core.DurableWaitPolicy{
			Kind:   core.WaitApproval,
			Prompt: func(json.RawMessage) (string, error) { return "Publish <draft> & open a PR?", nil },
			Target: func(raw json.RawMessage) (string, error) { return "blog-repo", nil },
		})
	agent, _ := core.New(&scripted{replies: []core.Message{
		core.AssistantMessage(core.ToolUseBlock{ID: "c1", Name: "open_pr", Input: json.RawMessage(`{"title":"Best CRMs for plumbers"}`)}),
		core.AssistantMessage(core.TextBlock{Text: "Opened PR #7."}),
	}}, core.AgentConfig{Tools: []core.Tool{tool}})
	def, _ := rt.Register("blog", "v1", agent)
	router, err := messaging.New(messaging.Config{Runtime: rt, Store: store, Channels: []messaging.Channel{bot}, Approvers: approvers})
	if err != nil {
		t.Fatal(err)
	}
	go router.Run(ctx)
	go bot.Poll(ctx, router)

	handle, err := rt.Submit(ctx, def, "Write and publish a post.")
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { text, _ := api.text(101); return text != "" })
	text, buttons := api.text(101)
	if !buttons || !strings.Contains(text, "Publish &lt;draft&gt; &amp; open a PR?") || !strings.Contains(text, "Best CRMs for plumbers") || !strings.Contains(text, "blog-repo") {
		t.Fatalf("notice text %q (buttons %v)", text, buttons)
	}
	var token string
	api.mu.Lock()
	for _, c := range api.calls {
		if c.Method == "sendMessage" {
			rows := c.Body["reply_markup"].(map[string]any)["inline_keyboard"].([]any)
			data := rows[0].([]any)[0].(map[string]any)["callback_data"].(string)
			if len(data) > 64 {
				t.Errorf("callback_data is %d bytes, Telegram allows 64", len(data))
			}
			token = strings.TrimPrefix(data, "a:")
		}
	}
	api.mu.Unlock()

	// A stranger's tap is refused with a toast and changes nothing.
	api.updates <- json.RawMessage(fmt.Sprintf(`{"update_id":1,"callback_query":{"id":"q0","from":{"id":666},"message":{"message_id":101,"chat":{"id":42}},"data":"a:%s"}}`, token))
	eventually(t, func() bool { api.mu.Lock(); defer api.mu.Unlock(); return len(api.answers) == 1 })
	if api.answers[0] != messaging.ErrNotApprover.Error() {
		t.Fatalf("stranger toast = %q", api.answers[0])
	}

	// The owner taps Approve, delivered by webhook this time.
	update := fmt.Sprintf(`{"update_id":2,"callback_query":{"id":"q1","from":{"id":42,"username":"taylor"},"message":{"message_id":101,"chat":{"id":42}},"data":"a:%s"}}`, token)
	resp := postWebhook(t, bot, router, "wrong", update)
	if resp != http.StatusUnauthorized {
		t.Fatalf("bad secret status = %d", resp)
	}
	if resp := postWebhook(t, bot, router, "s3cret", update); resp != http.StatusOK {
		t.Fatalf("webhook status = %d", resp)
	}
	result, err := handle.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "Opened PR #7." || published != 1 {
		t.Fatalf("output %q, published %d", result.Output, published)
	}
	eventually(t, func() bool { text, _ := api.text(101); return strings.Contains(text, "Approved") })
	text, buttons = api.text(101)
	if buttons || !strings.Contains(text, "telegram:42") {
		t.Fatalf("settled text %q (buttons %v)", text, buttons)
	}
}

func postWebhook(t *testing.T, bot *telegram.Bot, h telegram.Handler, secret, body string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/telegram", bytes.NewBufferString(body))
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
	rec := httptest.NewRecorder()
	bot.WebhookHandler(h).ServeHTTP(rec, req)
	return rec.Code
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := telegram.New(telegram.Config{ChatID: 1}); err == nil {
		t.Error("missing token accepted")
	}
	if _, err := telegram.New(telegram.Config{Token: "x"}); err == nil {
		t.Error("missing chat accepted")
	}
}

func TestWebhookRequiresSecret(t *testing.T) {
	bot, _ := telegram.New(telegram.Config{Token: "x", ChatID: 1})
	if code := postWebhook(t, bot, nil, "", `{}`); code != http.StatusUnauthorized {
		t.Fatalf("webhook without configured secret = %d", code)
	}
}

func TestAPIErrorDoesNotLeakToken(t *testing.T) {
	bot, _ := telegram.New(telegram.Config{Token: "SECRET-TOKEN", ChatID: 1, BaseURL: "http://127.0.0.1:1"})
	_, err := bot.Send(context.Background(), messaging.Outbound{Text: "hi"})
	if err == nil || strings.Contains(err.Error(), "SECRET-TOKEN") {
		t.Fatalf("err = %v", err)
	}
}

// scriptedHandler returns queued results per update, then success.
type scriptedHandler struct {
	mu      sync.Mutex
	results map[string][]error
	seen    []string
}

func (h *scriptedHandler) Handle(_ context.Context, in messaging.Inbound) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen = append(h.seen, in.DeliveryID)
	if queue := h.results[in.DeliveryID]; len(queue) > 0 {
		h.results[in.DeliveryID] = queue[1:]
		return "", queue[0]
	}
	return "ok", nil
}

func (h *scriptedHandler) deliveries() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.seen...)
}

var errTransient = errors.New("store temporarily unavailable")

func TestPollRetriesFailedUpdateBeforeLaterOnes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api, srv := newFakeAPI(t)
	bot, _ := telegram.New(telegram.Config{Token: "TEST", ChatID: 42, BaseURL: srv.URL, RetryDelay: 10 * time.Millisecond})
	h := &scriptedHandler{results: map[string][]error{
		"1": {errTransient, errTransient},  // fails twice, then succeeds
		"2": {messaging.ErrNotApprover},    // handled: never retried
		"3": {messaging.ErrAlreadySettled}, // handled: never retried
	}}
	api.updates <- json.RawMessage(`{"update_id":1,"message":{"message_id":1,"from":{"id":42},"chat":{"id":42},"text":"one"}}`)
	api.updates <- json.RawMessage(`{"update_id":2,"message":{"message_id":2,"from":{"id":7},"chat":{"id":7},"text":"two"}}`)
	api.updates <- json.RawMessage(`{"update_id":3,"callback_query":{"id":"q","from":{"id":42},"data":"a:tok"}}`)
	go bot.Poll(ctx, h)
	eventually(t, func() bool { return len(h.deliveries()) >= 5 })
	time.Sleep(50 * time.Millisecond)
	got := strings.Join(h.deliveries(), ",")
	if got != "1,1,1,2,3" {
		t.Fatalf("deliveries = %s, want 1,1,1,2,3 (the failed update retried before later ones, handled ones once)", got)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if last := api.offsets[len(api.offsets)-1]; last != 4 {
		t.Fatalf("final offset = %d, want 4", last)
	}
}

func TestToastFailureDoesNotRedeliverCommittedAction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	api, srv := newFakeAPI(t)
	api.failToast = true
	bot, _ := telegram.New(telegram.Config{Token: "TEST", ChatID: 42, BaseURL: srv.URL, RetryDelay: 10 * time.Millisecond})
	h := &scriptedHandler{results: map[string][]error{}}
	api.updates <- json.RawMessage(`{"update_id":9,"callback_query":{"id":"q","from":{"id":42},"data":"a:tok"}}`)
	go bot.Poll(ctx, h)
	eventually(t, func() bool { api.mu.Lock(); defer api.mu.Unlock(); return len(api.answers) == 1 })
	time.Sleep(100 * time.Millisecond)
	if got := h.deliveries(); len(got) != 1 {
		t.Fatalf("handled %d times, want once", len(got))
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if last := api.offsets[len(api.offsets)-1]; last != 10 {
		t.Fatalf("offset = %d, want 10", last)
	}
}

func TestWebhookAsksForRedeliveryOnTransientFailure(t *testing.T) {
	_, srv := newFakeAPI(t)
	bot, _ := telegram.New(telegram.Config{Token: "TEST", ChatID: 42, BaseURL: srv.URL, WebhookSecret: "s"})
	h := &scriptedHandler{results: map[string][]error{"5": {errTransient}, "6": {messaging.ErrUnknownNotice}}}
	msg := func(id int) string {
		return fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"from":{"id":42},"chat":{"id":42},"text":"hi"}}`, id, id)
	}
	if code := postWebhook(t, bot, h, "s", msg(5)); code != http.StatusServiceUnavailable {
		t.Fatalf("transient failure status = %d, want 503", code)
	}
	if code := postWebhook(t, bot, h, "s", msg(5)); code != http.StatusOK {
		t.Fatalf("redelivery status = %d, want 200", code)
	}
	if code := postWebhook(t, bot, h, "s", msg(6)); code != http.StatusOK {
		t.Fatalf("handled error status = %d, want 200", code)
	}
}
