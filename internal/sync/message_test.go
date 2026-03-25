package sync

import (
	"bytes"
	"testing"

	"github.com/develerltd/go-automerge/internal/types"
)

func TestMessageRoundTrip(t *testing.T) {
	hashes := []types.ChangeHash{makeHash("h1"), makeHash("h2")}
	bloom := BloomFromHashes(hashes)

	msg := &Message{
		Heads: []types.ChangeHash{makeHash("head1")},
		Need:  []types.ChangeHash{makeHash("need1")},
		Have: []Have{{
			LastSync: []types.ChangeHash{makeHash("sync1")},
			Bloom:    bloom,
		}},
		Changes: [][]byte{
			{0x01, 0x02, 0x03},
			{0x04, 0x05},
		},
		Version: MessageV1Type,
	}

	data := msg.Encode()
	if data[0] != MessageV1Type {
		t.Errorf("expected V1 type byte, got 0x%02x", data[0])
	}

	decoded, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}

	if len(decoded.Heads) != 1 {
		t.Errorf("expected 1 head, got %d", len(decoded.Heads))
	}
	if decoded.Heads[0] != msg.Heads[0] {
		t.Error("heads mismatch")
	}

	if len(decoded.Need) != 1 {
		t.Errorf("expected 1 need, got %d", len(decoded.Need))
	}

	if len(decoded.Have) != 1 {
		t.Errorf("expected 1 have, got %d", len(decoded.Have))
	}
	if len(decoded.Have[0].LastSync) != 1 {
		t.Errorf("expected 1 last_sync, got %d", len(decoded.Have[0].LastSync))
	}
	if decoded.Have[0].Bloom.NumEntries != bloom.NumEntries {
		t.Errorf("bloom entries mismatch: %d vs %d", decoded.Have[0].Bloom.NumEntries, bloom.NumEntries)
	}

	if len(decoded.Changes) != 2 {
		t.Errorf("expected 2 changes, got %d", len(decoded.Changes))
	}
	if !bytes.Equal(decoded.Changes[0], msg.Changes[0]) {
		t.Error("change 0 mismatch")
	}
	if !bytes.Equal(decoded.Changes[1], msg.Changes[1]) {
		t.Error("change 1 mismatch")
	}
}

func TestMessageEmptyRoundTrip(t *testing.T) {
	msg := &Message{
		Version: MessageV1Type,
	}

	data := msg.Encode()
	decoded, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("DecodeMessage: %v", err)
	}

	if len(decoded.Heads) != 0 || len(decoded.Need) != 0 ||
		len(decoded.Have) != 0 || len(decoded.Changes) != 0 {
		t.Error("expected all empty fields")
	}
}

func TestStateRoundTrip(t *testing.T) {
	state := NewState()
	state.SharedHeads = []types.ChangeHash{makeHash("s1"), makeHash("s2")}

	data := state.Encode()
	restored, err := DecodeState(data)
	if err != nil {
		t.Fatalf("DecodeState: %v", err)
	}

	if len(restored.SharedHeads) != 2 {
		t.Fatalf("expected 2 shared heads, got %d", len(restored.SharedHeads))
	}
	if restored.SharedHeads[0] != state.SharedHeads[0] {
		t.Error("shared head 0 mismatch")
	}
	if restored.SharedHeads[1] != state.SharedHeads[1] {
		t.Error("shared head 1 mismatch")
	}
}
