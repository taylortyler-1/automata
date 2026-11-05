package messaging

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/taylortyler-1/automata/core"
)

const (
	noticesBucket  = "messaging_notices"
	waitsBucket    = "messaging_waits"
	messagesBucket = "messaging_messages"
	repliesBucket  = "messaging_replies"
	metaBucket     = "messaging_meta"
	cursorKey      = "cursor"

	recordVersion = 1

	// Labels the router puts on runs it starts, so it can post the final
	// output back to the chat the message came from.
	LabelChannel = "messaging.channel"
	LabelChat    = "messaging.chat"
	LabelMessage = "messaging.message"
	LabelUser    = "messaging.user"

	maxArgumentChars = 1500
	feedPageLimit    = 64
)

// Config configures a [Router].
type Config struct {
	Runtime *core.Runtime
	// Store holds the router's records. Pass the runtime's own store so
	// notices and runs share one durable file.
	Store    core.Store
	Channels []Channel
	// Approvers lists who may answer waits and message routed agents. Also
	// install Approvers.Authorizer as the runtime's Authorizer.
	Approvers Approvers
	// Route, when set, turns approvers' plain messages into runs.
	Route *Route
	// Logger defaults to slog.Default.
	Logger *slog.Logger
	// RetryDelay is how long Run waits after a failed feed read or post
	// before trying again. Zero selects two seconds.
	RetryDelay time.Duration
}

// Router posts durable waits to chat and routes chat events back into the
// runtime. Create one with [New], run [Router.Run] in a goroutine, and feed
// channel events to [Router.Handle].
type Router struct {
	rt        *core.Runtime
	store     core.Store
	channels  map[string]Channel
	order     []string
	approvers Approvers
	route     *Route
	log       *slog.Logger
	retry     time.Duration

	// mu serializes record updates with the posts and edits they describe.
	mu sync.Mutex
}

type noticeRecord struct {
	Version      int                   `json:"version"`
	Notice       Notice                `json:"notice"`
	ActionDigest string                `json:"action_digest,omitempty"`
	Posted       map[string]MessageRef `json:"posted,omitempty"`
	Settled      map[string]bool       `json:"settled,omitempty"`
	Settlement   *Settlement           `json:"settlement,omitempty"`
}

type replyRecord struct {
	Version int        `json:"version"`
	Sent    MessageRef `json:"sent"`
}

// New validates cfg and returns a Router.
func New(cfg Config) (*Router, error) {
	if cfg.Runtime == nil {
		return nil, errors.New("messaging: runtime is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("messaging: store is required")
	}
	if len(cfg.Channels) == 0 {
		return nil, errors.New("messaging: at least one channel is required")
	}
	r := &Router{
		rt: cfg.Runtime, store: cfg.Store, channels: make(map[string]Channel),
		approvers: cfg.Approvers, route: cfg.Route, log: cfg.Logger, retry: cfg.RetryDelay,
	}
	for _, ch := range cfg.Channels {
		name := ch.Name()
		if name == "" {
			return nil, errors.New("messaging: channel name is required")
		}
		if _, dup := r.channels[name]; dup {
			return nil, fmt.Errorf("messaging: duplicate channel %q", name)
		}
		r.channels[name] = ch
		r.order = append(r.order, name)
	}
	if r.log == nil {
		r.log = slog.Default()
	}
	if r.retry <= 0 {
		r.retry = 2 * time.Second
	}
	return r, nil
}

// Run follows the runtime's change feed until ctx ends: it posts a notice for
// every pending approval or question, settles notices when their waits
// settle, and posts routed runs' final output. The feed cursor is stored, so
// a restart resumes where it stopped. On first start, or when retention has
// pruned the cursor, it posts notices for every run already waiting.
func (r *Router) Run(ctx context.Context) error {
	cursor, ok, err := r.loadCursor(ctx)
	if err != nil {
		return err
	}
	if !ok {
		if cursor, err = r.resyncUntilDone(ctx); err != nil {
			return err
		}
	}
	for {
		page, err := r.rt.WaitChanges(ctx, cursor, feedPageLimit, core.WithChangeEvents())
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, core.ErrEventGap) {
			r.log.Warn("messaging: change feed cursor pruned; resynchronizing", "cursor", cursor)
			if cursor, err = r.resyncUntilDone(ctx); err != nil {
				return err
			}
			continue
		}
		if errors.Is(err, core.ErrRuntimeClosed) {
			return err
		}
		if err != nil {
			r.log.Error("messaging: read change feed", "error", err)
			if !sleep(ctx, r.retry) {
				return ctx.Err()
			}
			continue
		}
		for _, change := range page.Changes {
			for {
				err := r.apply(ctx, change)
				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.log.Error("messaging: apply change", "run", change.RunID, "sequence", change.Sequence, "error", err)
				if !sleep(ctx, r.retry) {
					return ctx.Err()
				}
			}
			cursor = change.Sequence
			if err := r.saveCursor(ctx, cursor); err != nil {
				return err
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (r *Router) resyncUntilDone(ctx context.Context) (uint64, error) {
	for {
		cursor, err := r.resync(ctx)
		if err == nil || ctx.Err() != nil {
			return cursor, err
		}
		r.log.Error("messaging: resynchronize waiting runs", "error", err)
		if !sleep(ctx, r.retry) {
			return 0, ctx.Err()
		}
	}
}

// resync reads the feed head, then posts notices for every waiting run. Any
// change after the head is applied again by Run, which is harmless.
func (r *Router) resync(ctx context.Context) (uint64, error) {
	page, err := r.rt.Changes(ctx, 0, 1)
	if err != nil && !errors.Is(err, core.ErrEventGap) {
		return 0, err
	}
	head := page.Head
	filter := core.RunFilter{States: []core.RuntimeState{core.RuntimeWaiting}}
	var errs []error
	for {
		runs, err := r.rt.Runs(ctx, filter)
		if err != nil {
			return 0, err
		}
		for _, run := range runs.Runs {
			if err := r.syncRun(ctx, run.RunID); err != nil {
				errs = append(errs, fmt.Errorf("run %s: %w", run.RunID, err))
			}
		}
		if !runs.More {
			break
		}
		filter.After = runs.Next
	}
	if err := errors.Join(errs...); err != nil {
		return 0, err
	}
	return head, r.saveCursor(ctx, head)
}

func (r *Router) apply(ctx context.Context, change core.RunChange) error {
	if change.Pruned || change.EventsPruned {
		return nil
	}
	seen := make(map[string]bool)
	for _, event := range change.Events {
		if event.Kind != core.CommittedWait || seen[event.WaitID] {
			continue
		}
		if event.WaitKind != core.WaitApproval && event.WaitKind != core.WaitQuestion {
			continue
		}
		seen[event.WaitID] = true
		if err := r.syncWait(ctx, change.RunID, event.WaitID); err != nil {
			return err
		}
	}
	if change.State == core.RuntimeTerminal {
		return r.replyFinal(ctx, change.RunID)
	}
	return nil
}

func (r *Router) snapshot(ctx context.Context, runID string) (core.RunSnapshot, bool, error) {
	snap, err := r.rt.Handle(runID).Snapshot(ctx)
	if errors.Is(err, core.ErrRunNotFound) || errors.Is(err, core.ErrRunPruned) {
		return core.RunSnapshot{}, false, nil
	}
	return snap, err == nil, err
}

func (r *Router) syncRun(ctx context.Context, runID string) error {
	snap, ok, err := r.snapshot(ctx, runID)
	if !ok {
		return err
	}
	for _, wait := range snap.Waits {
		if err := r.syncWaitSnapshot(ctx, snap, wait); err != nil {
			return err
		}
	}
	return nil
}

func (r *Router) syncWait(ctx context.Context, runID, waitID string) error {
	snap, ok, err := r.snapshot(ctx, runID)
	if !ok {
		return err
	}
	for _, wait := range snap.Waits {
		if wait.ID == waitID {
			return r.syncWaitSnapshot(ctx, snap, wait)
		}
	}
	return nil
}

func (r *Router) syncWaitSnapshot(ctx context.Context, snap core.RunSnapshot, wait core.WaitSnapshot) error {
	if wait.Kind != core.WaitApproval && wait.Kind != core.WaitQuestion {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if wait.State == core.WaitPending {
		return r.post(ctx, snap, wait)
	}
	return r.settle(ctx, snap.RunID, wait)
}

// post records a notice for a pending wait, then posts it to every channel
// that does not have it yet. The record is written before posting, so a
// crash between the two reposts at most once per channel.
func (r *Router) post(ctx context.Context, snap core.RunSnapshot, wait core.WaitSnapshot) error {
	var rec noticeRecord
	err := r.store.Transaction(ctx, true, func(tx core.StoreTransaction) error {
		found, err := r.recordForWait(tx, snap.RunID, wait.ID, &rec)
		if err != nil || found {
			return err
		}
		token, err := newToken()
		if err != nil {
			return err
		}
		rec = noticeRecord{Version: recordVersion, Notice: noticeFor(token, snap, wait), ActionDigest: wait.ActionDigest}
		if err := putJSON(tx, noticesBucket, token, rec); err != nil {
			return err
		}
		return tx.Put(waitsBucket, waitKey(snap.RunID, wait.ID), []byte(token))
	})
	if err != nil {
		return err
	}
	if rec.Settlement != nil {
		return nil
	}
	var errs []error
	for _, name := range r.order {
		if _, done := rec.Posted[name]; done {
			continue
		}
		ref, err := r.channels[name].Notify(ctx, rec.Notice)
		if err != nil {
			errs = append(errs, fmt.Errorf("notify %s: %w", name, err))
			continue
		}
		ref.Channel = name
		if rec.Posted == nil {
			rec.Posted = make(map[string]MessageRef)
		}
		rec.Posted[name] = ref
		if err := r.store.Transaction(ctx, true, func(tx core.StoreTransaction) error {
			if err := putJSON(tx, noticesBucket, rec.Notice.Token, rec); err != nil {
				return err
			}
			return tx.Put(messagesBucket, messageKey(ref), []byte(rec.Notice.Token))
		}); err != nil {
			return err
		}
	}
	return errors.Join(errs...)
}

// settle edits every posted copy of a settled wait's notice to show the
// outcome.
func (r *Router) settle(ctx context.Context, runID string, wait core.WaitSnapshot) error {
	var rec noticeRecord
	var found bool
	err := r.store.Transaction(ctx, false, func(tx core.StoreTransaction) error {
		var err error
		found, err = r.recordForWait(tx, runID, wait.ID, &rec)
		return err
	})
	if err != nil || !found {
		return err
	}
	s := settlementFor(wait)
	rec.Settlement = &s
	var errs []error
	for _, name := range r.order {
		ref, posted := rec.Posted[name]
		if !posted || rec.Settled[name] {
			continue
		}
		if err := r.channels[name].Settle(ctx, ref, rec.Notice, s); err != nil {
			errs = append(errs, fmt.Errorf("settle %s: %w", name, err))
			continue
		}
		if rec.Settled == nil {
			rec.Settled = make(map[string]bool)
		}
		rec.Settled[name] = true
	}
	if err := r.store.Transaction(ctx, true, func(tx core.StoreTransaction) error {
		return putJSON(tx, noticesBucket, rec.Notice.Token, rec)
	}); err != nil {
		return err
	}
	return errors.Join(errs...)
}

// Handle applies one verified inbound event. The returned text is a short
// confirmation a channel may show the sender (for example as a button
// toast); errors such as [ErrNotApprover] and [ErrAlreadySettled] are meant
// to be shown too. Handle is idempotent for a redelivered event.
func (r *Router) Handle(ctx context.Context, in Inbound) (string, error) {
	if _, ok := r.channels[in.Channel]; !ok {
		return "", fmt.Errorf("messaging: unknown channel %q", in.Channel)
	}
	if !r.approvers.Allowed(in.Channel, in.User) {
		return "", ErrNotApprover
	}
	switch in.Kind {
	case InboundAction:
		rec, err := r.lookup(ctx, noticesBucket, in.Token)
		if err != nil {
			return "", err
		}
		return r.answerAction(ctx, rec, in)
	case InboundMessage:
		if in.ReplyTo != "" {
			token, found, err := r.get(ctx, messagesBucket, messageKey(MessageRef{Channel: in.Channel, Chat: in.Chat, ID: in.ReplyTo}))
			if err != nil {
				return "", err
			}
			if found {
				rec, err := r.lookup(ctx, noticesBucket, string(token))
				if err != nil {
					return "", err
				}
				return r.answerReply(ctx, rec, in)
			}
		}
		return r.submit(ctx, in)
	default:
		return "", fmt.Errorf("messaging: unknown inbound kind %q", in.Kind)
	}
}

func (r *Router) answerAction(ctx context.Context, rec noticeRecord, in Inbound) (string, error) {
	if rec.Notice.Kind != core.WaitApproval {
		return "", errors.New("reply to this message with your answer")
	}
	resolution := core.WaitResolution{Actor: Actor(in.Channel, in.User), ActionDigest: rec.ActionDigest}
	var done string
	switch in.Action {
	case ActionApprove:
		resolution.Decision, done = core.Allow, "Approved"
	case ActionDeny:
		resolution.Decision, resolution.Reason, done = core.Deny, "denied in "+in.Channel, "Denied"
	default:
		return "", fmt.Errorf("messaging: unknown action %q", in.Action)
	}
	return done, r.resolve(ctx, rec, resolution)
}

// answerReply treats a reply to a question as its answer, and a reply to an
// approval as a denial with that reason. Approving always takes the button,
// so free text can never grant an action.
func (r *Router) answerReply(ctx context.Context, rec noticeRecord, in Inbound) (string, error) {
	resolution := core.WaitResolution{Actor: Actor(in.Channel, in.User)}
	if rec.Notice.Kind == core.WaitApproval {
		resolution.Decision, resolution.Reason, resolution.ActionDigest = core.Deny, truncate(in.Text, 1000), rec.ActionDigest
		return "Denied with your reason.", r.resolve(ctx, rec, resolution)
	}
	answer, err := json.Marshal(in.Text)
	if err != nil {
		return "", err
	}
	resolution.Answer = answer
	return "Answer sent.", r.resolve(ctx, rec, resolution)
}

func (r *Router) resolve(ctx context.Context, rec noticeRecord, resolution core.WaitResolution) error {
	handle := r.rt.Handle(rec.Notice.RunID)
	err := handle.ResolveWait(ctx, rec.Notice.WaitID, resolution)
	switch {
	case errors.Is(err, core.ErrWaitConflict), errors.Is(err, core.ErrWaitStale), errors.Is(err, core.ErrWaitExpired):
		err = ErrAlreadySettled
	case errors.Is(err, core.ErrApprovalUnauthorized):
		err = ErrNotApprover
	case errors.Is(err, core.ErrWaitNotFound), errors.Is(err, core.ErrRunNotFound), errors.Is(err, core.ErrRunPruned):
		err = ErrUnknownNotice
	}
	// Settle now rather than waiting for the feed, so the person sees the
	// outcome at once; the feed's later pass finds it settled.
	if syncErr := r.syncWait(ctx, rec.Notice.RunID, rec.Notice.WaitID); syncErr != nil {
		r.log.Warn("messaging: settle after answer", "run", rec.Notice.RunID, "error", syncErr)
	}
	return err
}

func (r *Router) submit(ctx context.Context, in Inbound) (string, error) {
	if r.route == nil {
		return "", nil
	}
	if in.Text == "" || in.DeliveryID == "" {
		return "", nil
	}
	scope := "messaging/" + in.Channel
	opts := []core.SubmitOption{
		core.WithIdempotencyKey(scope, in.DeliveryID),
		core.WithLabels(map[string]string{
			LabelChannel: in.Channel, LabelChat: in.Chat, LabelMessage: in.MessageID, LabelUser: in.User,
		}),
	}
	if r.route.Conversation {
		ref := core.ConversationRef{Scope: scope, ID: in.Chat}
		conv, err := r.rt.Conversation(ctx, ref)
		if err != nil && !errors.Is(err, core.ErrConversationNotFound) {
			return "", err
		}
		opts = append(opts, core.WithConversation(ref, conv.Head))
	}
	if r.route.Deadline > 0 {
		// A redelivery computes a later deadline, so its admission conflicts
		// and is treated as the duplicate it is below.
		opts = append(opts, core.WithDeadline(time.Now().Add(r.route.Deadline)))
	}
	_, err := r.rt.Submit(ctx, r.route.Definition, in.Text, opts...)
	switch {
	case errors.Is(err, core.ErrConversationBusy):
		return "I'm still working on your last message.", nil
	case errors.Is(err, core.ErrAdmissionConflict):
		// A redelivered message whose first delivery was already admitted,
		// after the conversation moved on.
		return "", nil
	case err != nil:
		return "", err
	}
	return "", nil
}

// replyFinal posts a routed run's final output to the chat that started it,
// once.
func (r *Router) replyFinal(ctx context.Context, runID string) error {
	snap, ok, err := r.snapshot(ctx, runID)
	if !ok {
		return err
	}
	name, chat := snap.Labels[LabelChannel], snap.Labels[LabelChat]
	ch, known := r.channels[name]
	if !known || chat == "" || snap.Parent != nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, sent, err := r.get(ctx, repliesBucket, runID); err != nil || sent {
		return err
	}
	text := snap.Result.Output
	if snap.Failure != nil {
		text = "The run failed: " + snap.Failure.Message
	} else if text == "" {
		text = fmt.Sprintf("Finished (%s) with no text output.", snap.Result.Status)
	}
	ref, err := ch.Send(ctx, Outbound{Chat: chat, Text: text, ReplyTo: snap.Labels[LabelMessage]})
	if err != nil {
		return err
	}
	return r.store.Transaction(ctx, true, func(tx core.StoreTransaction) error {
		return putJSON(tx, repliesBucket, runID, replyRecord{Version: recordVersion, Sent: ref})
	})
}

// --- records ---------------------------------------------------------------

func (r *Router) recordForWait(tx core.StoreTransaction, runID, waitID string, rec *noticeRecord) (bool, error) {
	token, err := tx.Get(waitsBucket, waitKey(runID, waitID))
	if errors.Is(err, core.ErrStoreKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	raw, err := tx.Get(noticesBucket, string(token))
	if err != nil {
		return false, err
	}
	return true, decodeRecord(raw, rec)
}

func (r *Router) lookup(ctx context.Context, bucket, key string) (noticeRecord, error) {
	var rec noticeRecord
	raw, found, err := r.get(ctx, bucket, key)
	if err != nil {
		return rec, err
	}
	if !found {
		return rec, ErrUnknownNotice
	}
	return rec, decodeRecord(raw, &rec)
}

func (r *Router) get(ctx context.Context, bucket, key string) ([]byte, bool, error) {
	if key == "" {
		return nil, false, nil
	}
	var raw []byte
	err := r.store.Transaction(ctx, false, func(tx core.StoreTransaction) error {
		var err error
		raw, err = tx.Get(bucket, key)
		return err
	})
	if errors.Is(err, core.ErrStoreKeyNotFound) {
		return nil, false, nil
	}
	return raw, err == nil, err
}

func (r *Router) loadCursor(ctx context.Context) (uint64, bool, error) {
	raw, found, err := r.get(ctx, metaBucket, cursorKey)
	if err != nil || !found {
		return 0, false, err
	}
	var cursor uint64
	return cursor, true, json.Unmarshal(raw, &cursor)
}

func (r *Router) saveCursor(ctx context.Context, cursor uint64) error {
	return r.store.Transaction(ctx, true, func(tx core.StoreTransaction) error {
		return putJSON(tx, metaBucket, cursorKey, cursor)
	})
}

func decodeRecord(raw []byte, rec *noticeRecord) error {
	if err := json.Unmarshal(raw, rec); err != nil {
		return fmt.Errorf("messaging: decode notice: %w", err)
	}
	if rec.Version != recordVersion {
		return fmt.Errorf("messaging: unsupported notice version %d", rec.Version)
	}
	return nil
}

func putJSON(tx core.StoreTransaction, bucket, key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return tx.Put(bucket, key, raw)
}

func waitKey(runID, waitID string) string { return runID + "/" + waitID }

func messageKey(ref MessageRef) string { return ref.Channel + "\x00" + ref.Chat + "\x00" + ref.ID }

func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func noticeFor(token string, snap core.RunSnapshot, wait core.WaitSnapshot) Notice {
	n := Notice{
		Token: token, Kind: wait.Kind, RunID: snap.RunID, WaitID: wait.ID, Agent: snap.Definition.ID,
		Tool: wait.Tool, Prompt: wait.Prompt, Target: wait.Target, Arguments: formatArguments(wait.Arguments),
		ExpiresAt: wait.ExpiresAt,
	}
	if len(wait.ActionDigest) >= 12 {
		n.Digest = wait.ActionDigest[:12]
	}
	return n
}

func settlementFor(wait core.WaitSnapshot) Settlement {
	s := Settlement{
		State: wait.State, Decision: wait.Resolution.Decision, Actor: wait.Resolution.Actor,
		Reason: wait.Resolution.Reason, At: wait.ResolvedAt,
	}
	if len(wait.Resolution.Answer) > 0 {
		var text string
		if json.Unmarshal(wait.Resolution.Answer, &text) == nil {
			s.Answer = text
		} else {
			s.Answer = string(wait.Resolution.Answer)
		}
	}
	return s
}

func formatArguments(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var out bytes.Buffer
	if json.Indent(&out, raw, "", "  ") != nil {
		return truncate(string(raw), maxArgumentChars)
	}
	return truncate(out.String(), maxArgumentChars)
}

func truncate(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	runes := []rune(s)
	return string(runes[:max]) + "…"
}
