package messaging_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/taylortyler-1/automata/core"
	"github.com/taylortyler-1/automata/extensions/messaging"
)

func init() { slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil))) }

// scripted replays assistant turns in order.
type scripted struct {
	mu      sync.Mutex
	replies []core.Message
	seen    []core.Request
}

func (p *scripted) Invoke(_ context.Context, req core.Request) (core.Response, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, req)
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

func say(text string) core.Message { return core.AssistantMessage(core.TextBlock{Text: text}) }

func call(id, tool, input string) core.Message {
	return core.AssistantMessage(core.ToolUseBlock{ID: id, Name: tool, Input: json.RawMessage(input)})
}

// fakeChannel records posts, edits and sends.
type fakeChannel struct {
	mu       sync.Mutex
	name     string
	notices  []messaging.Notice
	settled  map[string]messaging.Settlement
	sent     []messaging.Outbound
	failNext int
	changed  chan struct{}
}

func newFake(name string) *fakeChannel {
	return &fakeChannel{name: name, settled: map[string]messaging.Settlement{}, changed: make(chan struct{}, 100)}
}

func (f *fakeChannel) Name() string { return f.name }

func (f *fakeChannel) Notify(_ context.Context, n messaging.Notice) (messaging.MessageRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failNext > 0 {
		f.failNext--
		return messaging.MessageRef{}, errors.New("temporarily down")
	}
	f.notices = append(f.notices, n)
	f.changed <- struct{}{}
	return messaging.MessageRef{Chat: "chat1", ID: fmt.Sprint(len(f.notices))}, nil
}

func (f *fakeChannel) Settle(_ context.Context, ref messaging.MessageRef, _ messaging.Notice, s messaging.Settlement) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settled[ref.ID] = s
	f.changed <- struct{}{}
	return nil
}

func (f *fakeChannel) Send(_ context.Context, out messaging.Outbound) (messaging.MessageRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, out)
	f.changed <- struct{}{}
	return messaging.MessageRef{Chat: out.Chat, ID: "s" + fmt.Sprint(len(f.sent))}, nil
}

func (f *fakeChannel) waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		f.mu.Lock()
		ok := cond()
		f.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-f.changed:
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("timed out waiting for channel state")
		}
	}
}

type fixture struct {
	rt     *core.Runtime
	store  core.Store
	ch     *fakeChannel
	router *messaging.Router
	def    core.DefinitionRef
	ran    *int
	cancel context.CancelFunc
	done   chan error
}

var approvers = messaging.Approvers{"fake": {"ana"}}

func publishTool(ran *int) core.Tool {
	return core.WithDurableWait(
		core.Func("publish", "Publish a post.", func(_ context.Context, in struct {
			Slug string `json:"slug"`
		}) (string, error) {
			*ran++
			return "published " + in.Slug, nil
		}),
		core.DurableWaitPolicy{
			Kind:   core.WaitApproval,
			Prompt: func(json.RawMessage) (string, error) { return "Publish this post?", nil },
			Target: func(raw json.RawMessage) (string, error) { return string(raw), nil },
		})
}

func askTool() core.Tool {
	return core.WithDurableWait(
		core.Func("ask", "Ask the owner.", func(context.Context, struct {
			Q string `json:"q"`
		}) (string, error) {
			return "", errors.New("never runs")
		}),
		core.DurableWaitPolicy{
			Kind:   core.WaitQuestion,
			Prompt: func(raw json.RawMessage) (string, error) { return string(raw), nil },
		})
}

func newFixture(t *testing.T, store core.Store, provider core.Provider, route bool) *fixture {
	t.Helper()
	ctx := context.Background()
	if store == nil {
		store = core.NewMemoryStore()
	}
	rt, err := core.NewRuntime(ctx, core.RuntimeConfig{Store: store, Authorizer: approvers.Authorizer(nil)})
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	agent, err := core.New(provider, core.AgentConfig{Tools: []core.Tool{publishTool(&ran), askTool()}})
	if err != nil {
		t.Fatal(err)
	}
	def, err := rt.Register("blog", "v1", agent)
	if err != nil {
		t.Fatal(err)
	}
	ch := newFake("fake")
	cfg := messaging.Config{Runtime: rt, Store: store, Channels: []messaging.Channel{ch}, Approvers: approvers, RetryDelay: 10 * time.Millisecond}
	if route {
		cfg.Route = &messaging.Route{Definition: def, Conversation: true}
	}
	router, err := messaging.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{rt: rt, store: store, ch: ch, router: router, def: def, ran: &ran, done: make(chan error, 1)}
	f.start()
	t.Cleanup(func() { f.stop(); rt.Close() })
	return f
}

func (f *fixture) start() {
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	done := make(chan error, 1)
	f.done = done
	go func() { done <- f.router.Run(ctx) }()
}

func (f *fixture) stop() {
	if f.cancel != nil {
		f.cancel()
		<-f.done
		f.cancel = nil
	}
}

func TestApprovalButtonApprovesExactAction(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, nil, &scripted{replies: []core.Message{call("c1", "publish", `{"slug":"crm-tools"}`), say("Done.")}}, false)
	handle, err := f.rt.Submit(ctx, f.def, "Publish the CRM post.")
	if err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.notices) == 1 })
	n := f.ch.notices[0]
	if n.Kind != core.WaitApproval || n.Tool != "publish" || n.Prompt != "Publish this post?" || len(n.Digest) != 12 {
		t.Fatalf("notice = %+v", n)
	}

	// Someone not on the list cannot approve.
	if _, err := f.router.Handle(ctx, messaging.Inbound{Channel: "fake", Kind: messaging.InboundAction, User: "mallory", Token: n.Token, Action: messaging.ActionApprove}); !errors.Is(err, messaging.ErrNotApprover) {
		t.Fatalf("stranger approve err = %v", err)
	}
	approve := messaging.Inbound{Channel: "fake", Kind: messaging.InboundAction, User: "ana", Token: n.Token, Action: messaging.ActionApprove}
	if text, err := f.router.Handle(ctx, approve); err != nil || text != "Approved" {
		t.Fatalf("approve = %q, %v", text, err)
	}
	// A double tap is the same answer and succeeds.
	if _, err := f.router.Handle(ctx, approve); err != nil {
		t.Fatalf("second approve: %v", err)
	}
	// A conflicting answer is reported as already settled.
	deny := approve
	deny.Action = messaging.ActionDeny
	if _, err := f.router.Handle(ctx, deny); !errors.Is(err, messaging.ErrAlreadySettled) {
		t.Fatalf("deny after approve err = %v", err)
	}
	result, err := handle.Await(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "Done." || *f.ran != 1 {
		t.Fatalf("output %q, tool ran %d times", result.Output, *f.ran)
	}
	f.ch.waitFor(t, func() bool { _, ok := f.ch.settled["1"]; return ok })
	if s := f.ch.settled["1"]; s.Decision != core.Allow || s.Actor != "fake:ana" {
		t.Fatalf("settlement = %+v", s)
	}
	if len(f.ch.notices) != 1 {
		t.Fatalf("posted %d notices, want 1", len(f.ch.notices))
	}
}

func TestReplyToApprovalDeniesWithReason(t *testing.T) {
	ctx := context.Background()
	provider := &scripted{replies: []core.Message{call("c1", "publish", `{"slug":"x"}`), say("Will revise.")}}
	f := newFixture(t, nil, provider, false)
	handle, err := f.rt.Submit(ctx, f.def, "Publish.")
	if err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.notices) == 1 })
	text, err := f.router.Handle(ctx, messaging.Inbound{Channel: "fake", Kind: messaging.InboundMessage, User: "ana", Chat: "chat1", ReplyTo: "1", Text: "Too salesy, tone it down", DeliveryID: "u1"})
	if err != nil || text == "" {
		t.Fatalf("reply = %q, %v", text, err)
	}
	if _, err := handle.Await(ctx); err != nil {
		t.Fatal(err)
	}
	if *f.ran != 0 {
		t.Fatal("denied tool ran")
	}
	// The model saw the reason as the tool result.
	last := provider.seen[len(provider.seen)-1]
	found := false
	for _, m := range last.Messages {
		for _, b := range m.Blocks {
			if r, ok := b.(core.ToolResultBlock); ok && strings.Contains(fmt.Sprint(r.Content), "Too salesy") {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("deny reason not returned to the model")
	}
	f.ch.waitFor(t, func() bool { return f.ch.settled["1"].Decision == core.Deny })
}

func TestQuestionReplyIsTheAnswer(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, nil, &scripted{replies: []core.Message{call("c1", "ask", `{"q":"Which niche?"}`), say("Got it.")}}, false)
	handle, err := f.rt.Submit(ctx, f.def, "Plan the blog.")
	if err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.notices) == 1 })
	if f.ch.notices[0].Kind != core.WaitQuestion {
		t.Fatalf("kind = %s", f.ch.notices[0].Kind)
	}
	// Buttons do not answer questions.
	if _, err := f.router.Handle(ctx, messaging.Inbound{Channel: "fake", Kind: messaging.InboundAction, User: "ana", Token: f.ch.notices[0].Token, Action: messaging.ActionApprove}); err == nil {
		t.Fatal("button answered a question")
	}
	if _, err := f.router.Handle(ctx, messaging.Inbound{Channel: "fake", Kind: messaging.InboundMessage, User: "ana", Chat: "chat1", ReplyTo: "1", Text: "Field service software", DeliveryID: "u2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Await(ctx); err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return f.ch.settled["1"].Answer == "Field service software" })
}

func TestMessageStartsConversationAndRepliesOnce(t *testing.T) {
	ctx := context.Background()
	provider := &scripted{replies: []core.Message{say("Hi! Want a post plan?"), say("Here is the plan.")}}
	f := newFixture(t, nil, provider, true)
	msg := messaging.Inbound{Channel: "fake", Kind: messaging.InboundMessage, User: "ana", Chat: "chat1", MessageID: "m1", Text: "hello", DeliveryID: "u10"}
	if _, err := f.router.Handle(ctx, msg); err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.sent) == 1 })
	if got := f.ch.sent[0]; got.Text != "Hi! Want a post plan?" || got.Chat != "chat1" || got.ReplyTo != "m1" {
		t.Fatalf("reply = %+v", got)
	}
	// A redelivery of the same update does not start another run.
	if _, err := f.router.Handle(ctx, msg); err != nil {
		t.Fatal(err)
	}
	second := msg
	second.MessageID, second.Text, second.DeliveryID = "m2", "yes please", "u11"
	if _, err := f.router.Handle(ctx, second); err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.sent) == 2 })
	conv, err := f.rt.Conversation(ctx, core.ConversationRef{Scope: "messaging/fake", ID: "chat1"})
	if err != nil || conv.Turns != 2 {
		t.Fatalf("conversation = %+v, %v", conv, err)
	}
	// The second turn saw the first.
	if n := len(provider.seen[1].Messages); n < 3 {
		t.Fatalf("second turn saw %d messages, want the earlier turn too", n)
	}
	// Strangers cannot start runs.
	if _, err := f.router.Handle(ctx, messaging.Inbound{Channel: "fake", Kind: messaging.InboundMessage, User: "mallory", Chat: "x", Text: "hi", DeliveryID: "u12"}); !errors.Is(err, messaging.ErrNotApprover) {
		t.Fatalf("stranger err = %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	f.ch.mu.Lock()
	defer f.ch.mu.Unlock()
	if len(f.ch.sent) != 2 {
		t.Fatalf("sent %d replies, want 2", len(f.ch.sent))
	}
}

func TestRestartDoesNotRepostAndResumesSettling(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryStore()
	f := newFixture(t, store, &scripted{replies: []core.Message{call("c1", "publish", `{"slug":"a"}`), say("ok")}}, false)
	handle, err := f.rt.Submit(ctx, f.def, "Publish.")
	if err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.notices) == 1 })
	token := f.ch.notices[0].Token

	// Restart the router against the same store: no second notice.
	f.stop()
	router2, err := messaging.New(messaging.Config{Runtime: f.rt, Store: store, Channels: []messaging.Channel{f.ch}, Approvers: approvers})
	if err != nil {
		t.Fatal(err)
	}
	f.router = router2
	f.start()
	time.Sleep(50 * time.Millisecond)

	// The wait is answered outside chat, for example from an HTTP API.
	snap, _ := handle.Snapshot(ctx)
	if err := handle.ResolveWait(ctx, snap.Waits[0].ID, core.WaitResolution{Decision: core.Deny, Reason: "no", Actor: "other", ActionDigest: snap.Waits[0].ActionDigest}); err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return f.ch.settled["1"].Decision == core.Deny })
	if len(f.ch.notices) != 1 {
		t.Fatalf("posted %d notices after restart, want 1", len(f.ch.notices))
	}
	if _, err := f.router.Handle(ctx, messaging.Inbound{Channel: "fake", Kind: messaging.InboundAction, User: "ana", Token: token, Action: messaging.ActionApprove}); !errors.Is(err, messaging.ErrAlreadySettled) {
		t.Fatalf("late approve err = %v", err)
	}
}

func TestFirstStartPostsExistingWaitsAndRetriesFailedPost(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryStore()
	rt, err := core.NewRuntime(ctx, core.RuntimeConfig{Store: store, Authorizer: approvers.Authorizer(nil)})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	ran := 0
	agent, _ := core.New(&scripted{replies: []core.Message{call("c1", "publish", `{"slug":"a"}`)}}, core.AgentConfig{Tools: []core.Tool{publishTool(&ran)}})
	def, _ := rt.Register("blog", "v1", agent)
	handle, err := rt.Submit(ctx, def, "Publish.")
	if err != nil {
		t.Fatal(err)
	}
	for {
		snap, _ := handle.Snapshot(ctx)
		if snap.State == core.RuntimeWaiting {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	ch := newFake("fake")
	ch.failNext = 1
	router, err := messaging.New(messaging.Config{Runtime: rt, Store: store, Channels: []messaging.Channel{ch}, Approvers: approvers, RetryDelay: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go router.Run(runCtx)
	// The first post fails; the resync retries until it lands, once.
	ch.waitFor(t, func() bool { return len(ch.notices) == 1 })
	time.Sleep(50 * time.Millisecond)
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.notices) != 1 || ch.failNext != 0 {
		t.Fatalf("notices = %d, failNext = %d", len(ch.notices), ch.failNext)
	}
}

func TestAuthorizerFallback(t *testing.T) {
	ctx := context.Background()
	fallback := core.ApprovalAuthorizerFunc(func(_ context.Context, c core.ApprovalAuthorization) error {
		if c.Actor == "http-admin" {
			return nil
		}
		return core.ErrApprovalUnauthorized
	})
	auth := approvers.Authorizer(fallback)
	for actor, want := range map[string]bool{"fake:ana": true, "fake:bob": false, "http-admin": true, "other": false, "slack:ana": false} {
		err := auth.AuthorizeApproval(ctx, core.ApprovalAuthorization{Actor: actor})
		if (err == nil) != want {
			t.Errorf("actor %q: err = %v, want allowed=%v", actor, err, want)
		}
	}
}
