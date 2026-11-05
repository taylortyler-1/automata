// Package telegram is a Telegram Bot API channel for
// [github.com/taylortyler-1/automata/extensions/messaging].
//
// Notices are sent to one chat (usually your private chat with the bot) with
// Approve and Deny buttons. Updates arrive by long polling with [Bot.Poll],
// which needs no public URL, or through [Bot.WebhookHandler] when the host
// already serves HTTPS. Only the standard library is used.
package telegram

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/taylortyler-1/automata/core"
	"github.com/taylortyler-1/automata/extensions/messaging"
)

const (
	defaultBaseURL = "https://api.telegram.org"
	maxTextChars   = 4000 // Telegram allows 4096 characters per message.
	pollTimeout    = 30 * time.Second
)

// Config configures a [Bot].
type Config struct {
	// Token is the bot token from @BotFather.
	Token string
	// ChatID is where notices are sent: your user ID for a private chat with
	// the bot, or a group's ID.
	ChatID int64
	// Name is the channel name used in [messaging.Approvers] and actor
	// strings. Defaults to "telegram".
	Name string
	// WebhookSecret is checked against the X-Telegram-Bot-Api-Secret-Token
	// header by [Bot.WebhookHandler]. Required for webhooks.
	WebhookSecret string
	// BaseURL overrides the Bot API address, for tests.
	BaseURL    string
	HTTPClient *http.Client
	Logger     *slog.Logger
	// Location formats times in notices. Defaults to UTC.
	Location *time.Location
	// RetryDelay is how long Poll waits before fetching again after a failed
	// fetch or a transient handling failure. Zero selects three seconds.
	RetryDelay time.Duration
}

// Handler receives verified inbound events; *messaging.Router implements it.
type Handler interface {
	Handle(context.Context, messaging.Inbound) (string, error)
}

// Bot is a Telegram channel.
type Bot struct {
	cfg  Config
	http *http.Client
	log  *slog.Logger
}

var _ messaging.Channel = (*Bot)(nil)

// New returns a Bot.
func New(cfg Config) (*Bot, error) {
	if cfg.Token == "" {
		return nil, errors.New("telegram: bot token is required")
	}
	if cfg.ChatID == 0 {
		return nil, errors.New("telegram: chat ID is required")
	}
	if cfg.Name == "" {
		cfg.Name = "telegram"
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
	}
	if cfg.Location == nil {
		cfg.Location = time.UTC
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 3 * time.Second
	}
	b := &Bot{cfg: cfg, http: cfg.HTTPClient, log: cfg.Logger}
	if b.http == nil {
		b.http = &http.Client{Timeout: pollTimeout + 15*time.Second}
	}
	if b.log == nil {
		b.log = slog.Default()
	}
	return b, nil
}

// Name implements [messaging.Channel].
func (b *Bot) Name() string { return b.cfg.Name }

// Notify implements [messaging.Channel].
func (b *Bot) Notify(ctx context.Context, n messaging.Notice) (messaging.MessageRef, error) {
	req := sendMessageRequest{ChatID: b.cfg.ChatID, Text: b.noticeText(n, nil), ParseMode: "HTML"}
	if n.Kind == core.WaitApproval {
		req.ReplyMarkup = &inlineKeyboard{Rows: [][]inlineButton{{
			{Text: "✅ Approve", CallbackData: "a:" + n.Token},
			{Text: "❌ Deny", CallbackData: "d:" + n.Token},
		}}}
	} else {
		req.ReplyMarkup = &forceReply{ForceReply: true, Placeholder: "Your answer"}
	}
	var msg message
	if err := b.call(ctx, "sendMessage", req, &msg); err != nil {
		return messaging.MessageRef{}, err
	}
	return b.ref(msg), nil
}

// Settle implements [messaging.Channel]: it rewrites the notice with its
// outcome and removes the buttons.
func (b *Bot) Settle(ctx context.Context, ref messaging.MessageRef, n messaging.Notice, s messaging.Settlement) error {
	chat, err := strconv.ParseInt(ref.Chat, 10, 64)
	if err != nil {
		return fmt.Errorf("telegram: chat ID %q: %w", ref.Chat, err)
	}
	id, err := strconv.ParseInt(ref.ID, 10, 64)
	if err != nil {
		return fmt.Errorf("telegram: message ID %q: %w", ref.ID, err)
	}
	req := editMessageTextRequest{ChatID: chat, MessageID: id, Text: b.noticeText(n, &s), ParseMode: "HTML"}
	err = b.call(ctx, "editMessageText", req, nil)
	var apiErr *APIError
	if errors.As(err, &apiErr) && strings.Contains(apiErr.Description, "message is not modified") {
		return nil
	}
	return err
}

// Send implements [messaging.Channel]. Text is sent as plain text.
func (b *Bot) Send(ctx context.Context, out messaging.Outbound) (messaging.MessageRef, error) {
	chat := b.cfg.ChatID
	if out.Chat != "" {
		parsed, err := strconv.ParseInt(out.Chat, 10, 64)
		if err != nil {
			return messaging.MessageRef{}, fmt.Errorf("telegram: chat ID %q: %w", out.Chat, err)
		}
		chat = parsed
	}
	req := sendMessageRequest{ChatID: chat, Text: truncate(out.Text, maxTextChars)}
	if id, err := strconv.ParseInt(out.ReplyTo, 10, 64); err == nil && id != 0 {
		req.ReplyParameters = &replyParameters{MessageID: id, AllowSendingWithoutReply: true}
	}
	var msg message
	if err := b.call(ctx, "sendMessage", req, &msg); err != nil {
		return messaging.MessageRef{}, err
	}
	return b.ref(msg), nil
}

// Poll long-polls getUpdates and passes each update to h until ctx ends. It
// deletes any configured webhook first, since Telegram allows only one of the
// two. An update is confirmed (the offset moves past it) only once h has
// handled it; after a transient failure Poll waits and fetches again from that
// update, so later updates are not processed ahead of it. The router's
// handling is idempotent, so a redelivered update is safe.
func (b *Bot) Poll(ctx context.Context, h Handler) error {
	if err := b.call(ctx, "deleteWebhook", struct{}{}, nil); err != nil {
		return fmt.Errorf("telegram: delete webhook before polling: %w", err)
	}
	var offset int64
	for {
		var updates []update
		err := b.call(ctx, "getUpdates", getUpdatesRequest{
			Offset: offset, Timeout: int(pollTimeout / time.Second),
			AllowedUpdates: []string{"message", "callback_query"},
		}, &updates)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			b.log.Warn("telegram: getUpdates", "error", err)
			if !b.pause(ctx) {
				return ctx.Err()
			}
			continue
		}
		for _, u := range updates {
			if err := b.dispatch(ctx, h, u); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				b.log.Warn("telegram: update not handled; will retry", "update", u.UpdateID, "error", err)
				if !b.pause(ctx) {
					return ctx.Err()
				}
				break
			}
			offset = u.UpdateID + 1
		}
	}
}

func (b *Bot) pause(ctx context.Context) bool {
	t := time.NewTimer(b.cfg.RetryDelay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// WebhookHandler serves Telegram webhook deliveries. Register it with
// setWebhook, passing the same secret_token as Config.WebhookSecret.
// Requests without the matching secret are rejected before parsing. A
// transient handling failure answers 503, so Telegram redelivers the update.
func (b *Bot) WebhookHandler(h Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("X-Telegram-Bot-Api-Secret-Token")
		if b.cfg.WebhookSecret == "" || subtle.ConstantTimeCompare([]byte(got), []byte(b.cfg.WebhookSecret)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var u update
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&u); err != nil {
			http.Error(w, "bad update", http.StatusBadRequest)
			return
		}
		if err := b.dispatch(r.Context(), h, u); err != nil {
			b.log.Warn("telegram: webhook update not handled; asking for redelivery", "update", u.UpdateID, "error", err)
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

// dispatch hands one update to h. It returns an error only when h failed
// transiently (see [messaging.Handled]) and the update must be redelivered.
// Failing to show the sender a confirmation never causes a redelivery: the
// action itself is already committed.
func (b *Bot) dispatch(ctx context.Context, h Handler, u update) error {
	delivery := strconv.FormatInt(u.UpdateID, 10)
	switch {
	case u.CallbackQuery != nil:
		q := u.CallbackQuery
		in := messaging.Inbound{
			Channel: b.cfg.Name, Kind: messaging.InboundAction, DeliveryID: delivery,
			User: strconv.FormatInt(q.From.ID, 10), Name: q.From.display(),
		}
		if q.Message != nil {
			in.Chat, in.MessageID = strconv.FormatInt(q.Message.Chat.ID, 10), strconv.FormatInt(q.Message.MessageID, 10)
		}
		action, token, _ := strings.Cut(q.Data, ":")
		in.Token = token
		switch action {
		case "a":
			in.Action = messaging.ActionApprove
		case "d":
			in.Action = messaging.ActionDeny
		}
		text, err := h.Handle(ctx, in)
		if !messaging.Handled(err) {
			// Leave the button spinning; the redelivery answers it.
			return err
		}
		if err != nil {
			text = err.Error()
		}
		if err := b.call(ctx, "answerCallbackQuery", answerCallbackRequest{CallbackQueryID: q.ID, Text: truncate(text, 190)}, nil); err != nil {
			b.log.Warn("telegram: answerCallbackQuery", "error", err)
		}
	case u.Message != nil && u.Message.From != nil && u.Message.Text != "":
		m := u.Message
		in := messaging.Inbound{
			Channel: b.cfg.Name, Kind: messaging.InboundMessage, DeliveryID: delivery,
			User: strconv.FormatInt(m.From.ID, 10), Name: m.From.display(),
			Chat: strconv.FormatInt(m.Chat.ID, 10), MessageID: strconv.FormatInt(m.MessageID, 10), Text: m.Text,
		}
		if m.ReplyTo != nil {
			in.ReplyTo = strconv.FormatInt(m.ReplyTo.MessageID, 10)
		}
		text, err := h.Handle(ctx, in)
		if !messaging.Handled(err) {
			return err
		}
		if errors.Is(err, messaging.ErrNotApprover) {
			// Strangers get no reply, so the bot does not advertise itself.
			b.log.Info("telegram: ignored message from unlisted user", "user", in.User)
			return nil
		}
		if err != nil {
			text = err.Error()
		}
		if text != "" {
			if _, err := b.Send(ctx, messaging.Outbound{Chat: in.Chat, Text: text, ReplyTo: in.MessageID}); err != nil {
				b.log.Warn("telegram: reply", "error", err)
			}
		}
	}
	return nil
}

func (b *Bot) ref(msg message) messaging.MessageRef {
	return messaging.MessageRef{
		Channel: b.cfg.Name, Chat: strconv.FormatInt(msg.Chat.ID, 10), ID: strconv.FormatInt(msg.MessageID, 10),
	}
}

func (b *Bot) noticeText(n messaging.Notice, s *messaging.Settlement) string {
	var t strings.Builder
	esc := html.EscapeString
	if n.Kind == core.WaitApproval {
		fmt.Fprintf(&t, "🔐 <b>Approval needed</b> · <code>%s</code>\n", esc(n.Agent))
	} else {
		fmt.Fprintf(&t, "❓ <b>Question</b> · <code>%s</code>\n", esc(n.Agent))
	}
	if n.Prompt != "" {
		fmt.Fprintf(&t, "\n%s\n", esc(truncate(n.Prompt, 1500)))
	}
	if n.Kind == core.WaitApproval {
		fmt.Fprintf(&t, "\nTool: <code>%s</code>\n", esc(n.Tool))
		if n.Target != "" && n.Target != strings.TrimSpace(compact(n.Arguments)) {
			fmt.Fprintf(&t, "Target: <code>%s</code>\n", esc(truncate(n.Target, 300)))
		}
		if n.Arguments != "" {
			fmt.Fprintf(&t, "<pre>%s</pre>\n", esc(n.Arguments))
		}
		if n.Digest != "" {
			fmt.Fprintf(&t, "Action <code>%s</code>", esc(n.Digest))
		}
		if !n.ExpiresAt.IsZero() {
			fmt.Fprintf(&t, " · expires %s", n.ExpiresAt.In(b.cfg.Location).Format("Jan 2 15:04 MST"))
		}
		t.WriteString("\n")
	}
	if s == nil {
		if n.Kind == core.WaitApproval {
			t.WriteString("\n<i>Reply to this message with a reason to deny it.</i>")
		} else {
			t.WriteString("\n<i>Reply to this message with your answer.</i>")
		}
		return t.String()
	}
	at := ""
	if !s.At.IsZero() {
		at = " at " + s.At.In(b.cfg.Location).Format("15:04 MST")
	}
	by := ""
	if s.Actor != "" {
		by = " by " + esc(s.Actor)
	}
	switch {
	case s.State == core.WaitExpired:
		t.WriteString("\n⌛ <b>Expired</b>")
	case s.State == core.WaitCancelled:
		t.WriteString("\n🚫 <b>Cancelled</b> (the run ended)")
	case s.Decision == core.Allow:
		fmt.Fprintf(&t, "\n✅ <b>Approved</b>%s%s", by, at)
	case s.Decision == core.Deny:
		fmt.Fprintf(&t, "\n❌ <b>Denied</b>%s%s", by, at)
		if s.Reason != "" {
			fmt.Fprintf(&t, "\nReason: %s", esc(truncate(s.Reason, 500)))
		}
	default:
		fmt.Fprintf(&t, "\n💬 <b>Answered</b>%s%s", by, at)
		if s.Answer != "" {
			fmt.Fprintf(&t, "\n%s", esc(truncate(s.Answer, 500)))
		}
	}
	return t.String()
}

func compact(indented string) string {
	var out bytes.Buffer
	if json.Compact(&out, []byte(indented)) != nil {
		return indented
	}
	return out.String()
}

func truncate(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max]) + "…"
}

// --- Bot API ---------------------------------------------------------------

// APIError is an error response from the Bot API.
type APIError struct {
	Method      string
	Code        int
	Description string
	RetryAfter  int
}

func (e *APIError) Error() string {
	return fmt.Sprintf("telegram %s: %d %s", e.Method, e.Code, e.Description)
}

type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Parameters  *struct {
		RetryAfter int `json:"retry_after"`
	} `json:"parameters"`
}

func (b *Bot) call(ctx context.Context, method string, body, result any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	url := b.cfg.BaseURL + "/bot" + b.cfg.Token + "/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.http.Do(req)
	if err != nil {
		// The URL holds the bot token; never surface it in an error.
		var urlErr interface{ Unwrap() error }
		if errors.As(err, &urlErr) {
			err = urlErr.Unwrap()
		}
		return fmt.Errorf("telegram %s: %w", method, err)
	}
	defer resp.Body.Close()
	var out apiResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&out); err != nil {
		return fmt.Errorf("telegram %s: decode response (HTTP %d): %w", method, resp.StatusCode, err)
	}
	if !out.OK {
		apiErr := &APIError{Method: method, Code: out.ErrorCode, Description: out.Description}
		if out.Parameters != nil {
			apiErr.RetryAfter = out.Parameters.RetryAfter
		}
		return apiErr
	}
	if result == nil {
		return nil
	}
	return json.Unmarshal(out.Result, result)
}

type sendMessageRequest struct {
	ChatID          int64            `json:"chat_id"`
	Text            string           `json:"text"`
	ParseMode       string           `json:"parse_mode,omitempty"`
	ReplyMarkup     any              `json:"reply_markup,omitempty"`
	ReplyParameters *replyParameters `json:"reply_parameters,omitempty"`
}

type editMessageTextRequest struct {
	ChatID    int64  `json:"chat_id"`
	MessageID int64  `json:"message_id"`
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode,omitempty"`
}

type replyParameters struct {
	MessageID                int64 `json:"message_id"`
	AllowSendingWithoutReply bool  `json:"allow_sending_without_reply,omitempty"`
}

type inlineKeyboard struct {
	Rows [][]inlineButton `json:"inline_keyboard"`
}

type inlineButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data"`
}

type forceReply struct {
	ForceReply  bool   `json:"force_reply"`
	Placeholder string `json:"input_field_placeholder,omitempty"`
}

type answerCallbackRequest struct {
	CallbackQueryID string `json:"callback_query_id"`
	Text            string `json:"text,omitempty"`
}

type getUpdatesRequest struct {
	Offset         int64    `json:"offset,omitempty"`
	Timeout        int      `json:"timeout"`
	AllowedUpdates []string `json:"allowed_updates,omitempty"`
}

type update struct {
	UpdateID      int64          `json:"update_id"`
	Message       *message       `json:"message,omitempty"`
	CallbackQuery *callbackQuery `json:"callback_query,omitempty"`
}

type message struct {
	MessageID int64    `json:"message_id"`
	From      *user    `json:"from,omitempty"`
	Chat      chat     `json:"chat"`
	Text      string   `json:"text,omitempty"`
	ReplyTo   *message `json:"reply_to_message,omitempty"`
}

type callbackQuery struct {
	ID      string   `json:"id"`
	From    user     `json:"from"`
	Message *message `json:"message,omitempty"`
	Data    string   `json:"data,omitempty"`
}

type user struct {
	ID        int64  `json:"id"`
	FirstName string `json:"first_name,omitempty"`
	Username  string `json:"username,omitempty"`
}

func (u user) display() string {
	if u.Username != "" {
		return "@" + u.Username
	}
	return u.FirstName
}

type chat struct {
	ID int64 `json:"id"`
}
