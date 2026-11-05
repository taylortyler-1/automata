package messaging_test

import (
	"context"
	"testing"
	"time"

	"github.com/taylortyler-1/automata/core"
	"github.com/taylortyler-1/automata/extensions/messaging"
)

func (f *fixture) restart(t *testing.T) {
	t.Helper()
	f.stop()
	router, err := messaging.New(messaging.Config{Runtime: f.rt, Store: f.store, Channels: []messaging.Channel{f.ch}, Approvers: approvers, RetryDelay: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	f.router = router
	f.start()
}

// submitRouted admits a run as if a chat message had started it.
func submitRouted(t *testing.T, f *fixture, msg string) *core.RunHandle {
	t.Helper()
	handle, err := f.rt.Submit(context.Background(), f.def, "hi", core.WithLabels(map[string]string{
		messaging.LabelChannel: "fake", messaging.LabelChat: "chat1", messaging.LabelMessage: msg,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	return handle
}

func pendingNotice(t *testing.T, f *fixture) (*core.RunHandle, core.WaitSnapshot) {
	t.Helper()
	ctx := context.Background()
	handle, err := f.rt.Submit(ctx, f.def, "Publish.")
	if err != nil {
		t.Fatal(err)
	}
	f.ch.waitFor(t, func() bool { return len(f.ch.notices) == 1 })
	snap, err := handle.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return handle, snap.Waits[0]
}

func deny(t *testing.T, handle *core.RunHandle, wait core.WaitSnapshot) {
	t.Helper()
	ctx := context.Background()
	if err := handle.ResolveWait(ctx, wait.ID, core.WaitResolution{Decision: core.Deny, Reason: "no", Actor: "http", ActionDigest: wait.ActionDigest}); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Await(ctx); err != nil {
		t.Fatal(err)
	}
}

func prune(t *testing.T, f *fixture, policy core.RetentionPolicy) {
	t.Helper()
	time.Sleep(5 * time.Millisecond)
	if _, err := f.rt.Prune(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) sentCount() int {
	f.ch.mu.Lock()
	defer f.ch.mu.Unlock()
	return len(f.ch.sent)
}

func TestFirstStartRepliesToFinishedRoutedRuns(t *testing.T) {
	store := core.NewMemoryStore()
	f := newFixture(t, store, &scripted{replies: []core.Message{say("answer one")}}, false)
	f.stop()
	// A routed run finished before the router ever ran (or with its records
	// lost): first start must still deliver the answer, once.
	if err := store.Transaction(context.Background(), true, func(tx core.StoreTransaction) error {
		return tx.Delete("messaging_meta", "cursor")
	}); err != nil {
		t.Fatal(err)
	}
	submitRouted(t, f, "m1")
	f.restart(t)
	f.ch.waitFor(t, func() bool { return len(f.ch.sent) == 1 })
	if got := f.ch.sent[0]; got.Text != "answer one" || got.ReplyTo != "m1" {
		t.Fatalf("reply = %+v", got)
	}
	f.restart(t)
	time.Sleep(50 * time.Millisecond)
	if n := f.sentCount(); n != 1 {
		t.Fatalf("sent %d replies after a second restart, want 1", n)
	}
}

func TestRestartSettlesNoticeResolvedWhileStopped(t *testing.T) {
	f := newFixture(t, nil, &scripted{replies: []core.Message{call("c1", "publish", `{"slug":"a"}`), say("ok")}}, false)
	handle, wait := pendingNotice(t, f)
	f.stop()
	deny(t, handle, wait)
	f.restart(t)
	f.ch.waitFor(t, func() bool { return f.ch.settled["1"].Decision == core.Deny })
}

func TestRetentionGapDoesNotMissSettlementsOrReplies(t *testing.T) {
	f := newFixture(t, nil, &scripted{replies: []core.Message{
		call("c1", "publish", `{"slug":"a"}`), say("revised"), say("routed answer"),
	}}, false)
	handle, wait := pendingNotice(t, f)
	f.stop()
	// While the router is down the wait is answered elsewhere and a routed
	// run finishes; then retention removes the feed entries and events the
	// router's cursor pointed into.
	deny(t, handle, wait)
	submitRouted(t, f, "m7")
	prune(t, f, core.RetentionPolicy{Changes: time.Nanosecond, Events: time.Nanosecond})
	if _, err := f.rt.Changes(context.Background(), 1, 1); err == nil {
		t.Fatal("expected the old cursor to be outside the retained feed")
	}
	f.restart(t)
	f.ch.waitFor(t, func() bool { return f.ch.settled["1"].Decision == core.Deny && len(f.ch.sent) == 1 })
	if got := f.ch.sent[0]; got.Text != "routed answer" {
		t.Fatalf("reply = %+v", got)
	}
}

func TestPrunedEventsAreReconciledFromSnapshot(t *testing.T) {
	f := newFixture(t, nil, &scripted{replies: []core.Message{say("kept answer")}}, false)
	time.Sleep(20 * time.Millisecond) // let the router store a cursor
	f.stop()
	submitRouted(t, f, "m3")
	// The feed entries survive but the run's events do not.
	prune(t, f, core.RetentionPolicy{Events: time.Nanosecond})
	f.restart(t)
	f.ch.waitFor(t, func() bool { return len(f.ch.sent) == 1 })
}

func TestNoticeForPrunedRunIsSettled(t *testing.T) {
	f := newFixture(t, nil, &scripted{replies: []core.Message{call("c1", "publish", `{"slug":"a"}`), say("ok")}}, false)
	handle, wait := pendingNotice(t, f)
	f.stop()
	deny(t, handle, wait)
	prune(t, f, core.RetentionPolicy{Runs: time.Nanosecond, Changes: time.Nanosecond, Events: time.Nanosecond, History: time.Nanosecond})
	if _, err := handle.Snapshot(context.Background()); err == nil {
		t.Fatal("expected the run to be pruned")
	}
	f.restart(t)
	f.ch.waitFor(t, func() bool { _, ok := f.ch.settled["1"]; return ok })
	if s := f.ch.settled["1"]; s.State != core.WaitCancelled {
		t.Fatalf("settlement = %+v, want cancelled for a run that is gone", s)
	}
}
