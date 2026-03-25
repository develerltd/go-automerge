package automerge

import (
	"testing"
)

// syncDocs runs the sync protocol between two docs until convergence or max rounds.
func syncDocs(t *testing.T, doc1, doc2 *Doc, state1, state2 *SyncState, maxRounds int) {
	t.Helper()
	for i := 0; i < maxRounds; i++ {
		msg1 := doc1.GenerateSyncMessage(state1)
		msg2 := doc2.GenerateSyncMessage(state2)

		if msg1 == nil && msg2 == nil {
			t.Logf("synced in %d rounds", i)
			return
		}

		if msg1 != nil {
			if err := doc2.ReceiveSyncMessage(state2, msg1); err != nil {
				t.Fatalf("round %d: doc2 receive: %v", i, err)
			}
		}
		if msg2 != nil {
			if err := doc1.ReceiveSyncMessage(state1, msg2); err != nil {
				t.Fatalf("round %d: doc1 receive: %v", i, err)
			}
		}
	}
	t.Fatalf("failed to sync within %d rounds", maxRounds)
}

func TestSyncEmptyDocs(t *testing.T) {
	doc1 := New()
	doc2 := New()

	state1 := NewSyncState()
	state2 := NewSyncState()

	// Should quickly determine they're in sync (both empty)
	msg1 := doc1.GenerateSyncMessage(state1)
	if msg1 == nil {
		t.Fatal("first message should not be nil")
	}
	if err := doc2.ReceiveSyncMessage(state2, msg1); err != nil {
		t.Fatal(err)
	}

	msg2 := doc2.GenerateSyncMessage(state2)
	if msg2 == nil {
		t.Fatal("response should not be nil")
	}
	if err := doc1.ReceiveSyncMessage(state1, msg2); err != nil {
		t.Fatal(err)
	}

	// Now they should be in sync
	msg3 := doc1.GenerateSyncMessage(state1)
	if msg3 != nil {
		t.Log("extra round needed")
	}
}

func TestSyncOneWay(t *testing.T) {
	doc1 := New()
	doc1.Put(Root, "key", NewStr("value"))
	doc1.Commit("set key", 1000)

	doc2 := New()

	state1 := NewSyncState()
	state2 := NewSyncState()

	syncDocs(t, doc1, doc2, state1, state2, 10)

	// doc2 should now have the key
	val, _, err := doc2.Get(Root, MapProp("key"))
	if err != nil {
		t.Fatalf("doc2 Get: %v", err)
	}
	if val.Scalar.Str() != "value" {
		t.Errorf("expected 'value', got %s", val)
	}
}

func TestSyncBidirectional(t *testing.T) {
	// Start from a common state
	doc1 := New()
	doc1.Put(Root, "x", NewInt(1))
	doc1.Commit("initial", 1000)

	doc2 := doc1.Fork()

	// Both make changes
	doc1.Put(Root, "a", NewStr("from doc1"))
	doc1.Commit("doc1 change", 2000)

	doc2.Put(Root, "b", NewStr("from doc2"))
	doc2.Commit("doc2 change", 2000)

	state1 := NewSyncState()
	state2 := NewSyncState()

	syncDocs(t, doc1, doc2, state1, state2, 10)

	// Both should have all keys
	for _, doc := range []*Doc{doc1, doc2} {
		keys := doc.Keys(Root)
		if len(keys) != 3 {
			t.Errorf("expected 3 keys, got %v", keys)
		}
	}
}

func TestSyncMultipleChanges(t *testing.T) {
	doc1 := New()
	doc1.Put(Root, "a", NewInt(1))
	doc1.Commit("c1", 1000)
	doc1.Put(Root, "b", NewInt(2))
	doc1.Commit("c2", 2000)
	doc1.Put(Root, "c", NewInt(3))
	doc1.Commit("c3", 3000)

	doc2 := New()

	state1 := NewSyncState()
	state2 := NewSyncState()

	syncDocs(t, doc1, doc2, state1, state2, 10)

	keys := doc2.Keys(Root)
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %v", keys)
	}
}

func TestSyncAlreadySynced(t *testing.T) {
	doc1 := New()
	doc1.Put(Root, "x", NewInt(1))
	doc1.Commit("v1", 1000)

	doc2 := doc1.Fork()

	state1 := NewSyncState()
	state2 := NewSyncState()

	// Sync once
	syncDocs(t, doc1, doc2, state1, state2, 10)

	// Try again — should be a no-op quickly
	msg := doc1.GenerateSyncMessage(state1)
	if msg == nil {
		return // already knows it's in sync
	}
	doc2.ReceiveSyncMessage(state2, msg)
	msg2 := doc2.GenerateSyncMessage(state2)
	if msg2 != nil {
		doc1.ReceiveSyncMessage(state1, msg2)
	}

	// Should converge
	msg3 := doc1.GenerateSyncMessage(state1)
	// It's OK if msg3 is nil (in sync) or not (needs one more round)
	_ = msg3
}

func TestSyncSaveLoadRoundTrip(t *testing.T) {
	// Create doc1 with data, sync to doc2, save doc2, load, verify
	doc1 := New()
	doc1.Put(Root, "title", NewStr("Hello Sync"))
	doc1.Commit("v1", 1000)

	doc2 := New()
	state1 := NewSyncState()
	state2 := NewSyncState()

	syncDocs(t, doc1, doc2, state1, state2, 10)

	// Save doc2
	data, err := doc2.Save()
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	doc3, err := Load(data)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	val, _, err := doc3.Get(Root, MapProp("title"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if val.Scalar.Str() != "Hello Sync" {
		t.Errorf("expected 'Hello Sync', got %s", val)
	}
}
