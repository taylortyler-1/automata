package core_test

import (
	"testing"

	"github.com/taylortyler-1/automata/core"
	"github.com/taylortyler-1/automata/core/storetest"
)

// The in-memory ephemeral store is a full Store implementation and must pass
// the same contract suite as real adapters.
func TestMemoryStoreConformance(t *testing.T) {
	storetest.Conformance(t, func(t *testing.T) core.Store {
		return core.NewMemoryStore()
	})
}
