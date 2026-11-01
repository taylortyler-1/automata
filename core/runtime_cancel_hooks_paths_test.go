package core

import (
	"context"
	"sync"
	"testing"
)

// countingHooks returns one committed-run hook that records each run it sees
// with the status it observed.
func countingHooks() ([]CommittedRunHook, func() map[string][]RunStatus) {
	var mu sync.Mutex
	seen := map[string][]RunStatus{}
	hooks := []CommittedRunHook{{Name: "audit", Handle: func(_ context.Context, snapshot RunSnapshot) error {
		mu.Lock()
		defer mu.Unlock()
		seen[snapshot.RunID] = append(seen[snapshot.RunID], snapshot.Result.Status)
		return nil
	}}}
	return hooks, func() map[string][]RunStatus {
		mu.Lock()
		defer mu.Unlock()
		out := map[string][]RunStatus{}
		for id, statuses := range seen {
			out[id] = append([]RunStatus(nil), statuses...)
		}
		return out
	}
}

func assertCanceledOnceWithHook(t *testing.T, runtime *Runtime, runID string, seen map[string][]RunStatus) {
	t.Helper()
	if got := seen[runID]; len(got) != 1 || got[0] != RunCancelled {
		t.Fatalf("hooks saw run %s as %v, want exactly one cancelled delivery", runID, got)
	}
	snapshot, err := runtime.Handle(runID).Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != RuntimeTerminal || snapshot.Result.Status != RunCancelled || len(snapshot.Hooks) != 1 || snapshot.Hooks[0].Error != "" {
		t.Fatalf("run %s = %s %s hooks %+v", runID, snapshot.State, snapshot.Result.Status, snapshot.Hooks)
	}
}

// Directly canceling a ready run that no worker owns finalizes it through
// committed-run hooks before Cancel returns (F7).
func TestRuntimeCancelOfReadyRunDeliversHooks(t *testing.T) {
	hooks, seen := countingHooks()
	store := NewMemoryStore()
	ready := storedRuntimeRun{
		Version: runtimeEncodingVersion, RunID: "ready-run", DefinitionID: "agent",
		DefinitionRevision: "v1", Task: "work", State: RuntimeReady,
		Generation: 1, Result: RunResult{RunID: "ready-run"},
	}
	if err := store.Transaction(context.Background(), true, func(tx StoreTransaction) error {
		return putRuntimeRun(tx, ready)
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(context.Background(), RuntimeConfig{Store: store, Hooks: hooks})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if err := runtime.Handle(ready.RunID).Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertCanceledOnceWithHook(t, runtime, ready.RunID, seen())
}

// Directly canceling a run in needs-attention finalizes it through
// committed-run hooks before Cancel returns (F7).
func TestRuntimeCancelOfNeedsAttentionRunDeliversHooks(t *testing.T) {
	hooks, seen := countingHooks()
	runtime, err := NewRuntime(context.Background(), RuntimeConfig{Store: NewMemoryStore(), Hooks: hooks})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	ready := storedRuntimeRun{
		Version: runtimeEncodingVersion, RunID: "attention-run", DefinitionID: "agent",
		DefinitionRevision: "missing", Task: "work", State: RuntimeReady,
		Generation: 1, Result: RunResult{RunID: "attention-run"},
	}
	if err := runtime.transaction(context.Background(), true, func(tx StoreTransaction) error {
		return putRuntimeRun(tx, ready)
	}); err != nil {
		t.Fatal(err)
	}
	// Recover parks a run whose definition is not registered in attention.
	if err := runtime.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Handle(ready.RunID).Snapshot(context.Background())
	if err != nil || snapshot.State != RuntimeNeedsAttention {
		t.Fatalf("setup snapshot = %s, %v; want needs-attention", snapshot.State, err)
	}
	if err := runtime.Handle(ready.RunID).Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertCanceledOnceWithHook(t, runtime, ready.RunID, seen())
}

// A run whose cancellation was requested while running, and whose worker
// stopped before finalizing, is canceled by Recover through committed-run
// hooks, exactly once (F7, cancellation during recovery).
func TestRuntimeRecoverOfCancelRequestedRunDeliversHooks(t *testing.T) {
	hooks, seen := countingHooks()
	store := NewMemoryStore()
	messages := []Message{UserMessage("work")}
	record := storedRuntimeRun{
		Version: runtimeEncodingVersion, RunID: "cancel-requested", DefinitionID: "agent",
		DefinitionRevision: "v1", Task: "work", State: RuntimeCancelRequested,
		Generation: 2, Result: RunResult{RunID: "cancel-requested"},
		LastTransition: transitionProviderAttemptStarted,
	}
	if err := store.Transaction(context.Background(), true, func(tx StoreTransaction) error {
		if err := appendTranscript(tx, record.RunID, &record, messages); err != nil {
			return err
		}
		return putRuntimeRun(tx, record)
	}); err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime(context.Background(), RuntimeConfig{Store: store, Hooks: hooks})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if _, err := runtime.Register("agent", "v1", testAgent(&scriptedProvider{})); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A second pass and a late Cancel must not redeliver.
	if err := runtime.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Handle(record.RunID).Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertCanceledOnceWithHook(t, runtime, record.RunID, seen())
}
