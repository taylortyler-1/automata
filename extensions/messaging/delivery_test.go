package messaging_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/taylortyler-1/automata/core"
	"github.com/taylortyler-1/automata/extensions/messaging"
)

// flakyStore fails the next n transactions, like a briefly unavailable disk.
type flakyStore struct {
	core.Store
	mu   sync.Mutex
	fail int
}

var errStoreDown = errors.New("store temporarily unavailable")

func (s *flakyStore) failNext(n int) {
	s.mu.Lock()
	s.fail = n
	s.mu.Unlock()
}

func (s *flakyStore) Transaction(ctx context.Context, write bool, fn func(core.StoreTransaction) error) error {
	s.mu.Lock()
	if s.fail > 0 {
		s.fail--
		s.mu.Unlock()
		return errStoreDown
	}
	s.mu.Unlock()
	return s.Store.Transaction(ctx, write, fn)
}

// flakyRouter is a second router over the fixture's records whose own store
// access can be made to fail; the fixture's router keeps following the feed.
func flakyRouter(t *testing.T, f *fixture, route bool) (*messaging.Router, *flakyStore) {
	t.Helper()
	store := &flakyStore{Store: f.store}
	cfg := messaging.Config{Runtime: f.rt, Store: store, Channels: []messaging.Channel{f.ch}, Approvers: approvers}
	if route {
		cfg.Route = &messaging.Route{Definition: f.def, Conversation: true}
	}
	router, err := messaging.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return router, store
}

func TestHandledClassifiesErrors(t *testing.T) {
	for _, err := range []error{nil, messaging.ErrNotApprover, messaging.ErrUnknownNotice, messaging.ErrAlreadySettled,
		fmt.Errorf("%w: unknown action", messaging.ErrInvalidAction)} {
		if !messaging.Handled(err) {
			t.Errorf("Handled(%v) = false, want true", err)
		}
	}
	if messaging.Handled(errStoreDown) {
		t.Error("a store failure was treated as handled")
	}
}

func TestStoreFailureOnApprovalIsTransientAndRetrySucceeds(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, nil, &scripted{replies: []core.Message{call("c1", "publish", `{"slug":"a"}`), say("Done.")}}, false)
	handle, err := f.rt.Submit(ctx, f.def, "Publish.")
	if err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.notices) == 1 })
	router, store := flakyRouter(t, f, false)
	approve := messaging.Inbound{Channel: "fake", Kind: messaging.InboundAction, DeliveryID: "u1", User: "ana", Token: f.ch.notices[0].Token, Action: messaging.ActionApprove}

	store.failNext(1)
	if _, err := router.Handle(ctx, approve); err == nil || messaging.Handled(err) {
		t.Fatalf("approve during store failure: err = %v, want a transient error", err)
	}
	if snap, _ := handle.Snapshot(ctx); snap.Waits[0].State != core.WaitPending {
		t.Fatalf("wait state after failed delivery = %s, want pending", snap.Waits[0].State)
	}
	// The platform redelivers the same update.
	if text, err := router.Handle(ctx, approve); err != nil || text != "Approved" {
		t.Fatalf("redelivered approve = %q, %v", text, err)
	}
	// And once more, as after a lost acknowledgement: still a success.
	if _, err := router.Handle(ctx, approve); err != nil {
		t.Fatalf("second redelivery: %v", err)
	}
	if result, err := handle.Await(ctx); err != nil || result.Output != "Done." || *f.ran != 1 {
		t.Fatalf("result %q, err %v, tool ran %d times", result.Output, err, *f.ran)
	}
}

func TestRedeliveredMessageAdmitsOneRun(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, nil, &scripted{replies: []core.Message{say("first"), say("second")}}, true)
	router, store := flakyRouter(t, f, true)
	// A reply to the bot's earlier answer: the router first checks whether
	// it answers a notice, which needs its store.
	first := messaging.Inbound{Channel: "fake", Kind: messaging.InboundMessage, DeliveryID: "u1", User: "ana", Chat: "chat1", MessageID: "m1", ReplyTo: "s0", Text: "hello"}

	store.failNext(1)
	if _, err := router.Handle(ctx, first); err == nil || messaging.Handled(err) {
		t.Fatalf("message during store failure: err = %v, want a transient error", err)
	}
	if runs, _ := f.rt.Runs(ctx, core.RunFilter{}); len(runs.Runs) != 0 {
		t.Fatalf("a failed delivery admitted %d runs", len(runs.Runs))
	}
	// Redelivery after the failure admits the run.
	if _, err := router.Handle(ctx, first); err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.sent) == 1 })

	// A second message moves the conversation on.
	second := first
	second.DeliveryID, second.MessageID, second.Text = "u2", "m2", "again"
	if _, err := router.Handle(ctx, second); err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.sent) == 2 })

	// A late redelivery of the first update is a duplicate admission: it is
	// handled and starts nothing.
	if _, err := router.Handle(ctx, first); !messaging.Handled(err) {
		t.Fatalf("late redelivery err = %v", err)
	}
	runs, err := f.rt.Runs(ctx, core.RunFilter{Labels: map[string]string{messaging.LabelChat: "chat1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs.Runs) != 2 {
		t.Fatalf("admitted %d runs, want 2", len(runs.Runs))
	}
}
