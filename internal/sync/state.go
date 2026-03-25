package sync

import (
	"fmt"

	"github.com/develerltd/go-automerge/internal/encoding"
	"github.com/develerltd/go-automerge/internal/types"
)

// State tracks the sync state between two peers.
type State struct {
	// Persistent (survives across sessions)
	SharedHeads []types.ChangeHash

	// Ephemeral (per session)
	LastSentHeads []types.ChangeHash
	TheirHeads    []types.ChangeHash
	TheirNeed     []types.ChangeHash
	TheirHave     []Have
	SentHashes    map[types.ChangeHash]bool
	InFlight      bool
	HaveResponded bool

	// Track whether their_* fields have been set (nil vs empty)
	theirHeadsSet bool
	theirNeedSet  bool
	theirHaveSet  bool
}

// NewState creates a new empty sync state.
func NewState() *State {
	return &State{
		SentHashes: make(map[types.ChangeHash]bool),
	}
}

// Encode persists the sync state (only SharedHeads are persisted).
func (s *State) Encode() []byte {
	var buf []byte
	buf = append(buf, SyncStateType)
	buf = encodeHashes(buf, s.SharedHeads)
	return buf
}

// DecodeState restores a sync state from persisted bytes.
func DecodeState(data []byte) (*State, error) {
	if len(data) == 0 {
		return NewState(), nil
	}
	r := encoding.NewReader(data)

	typeByte, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("reading type byte: %w", err)
	}
	if typeByte != SyncStateType {
		return nil, fmt.Errorf("invalid sync state type: 0x%02x", typeByte)
	}

	heads, err := decodeHashes(r)
	if err != nil {
		return nil, fmt.Errorf("reading shared heads: %w", err)
	}

	state := NewState()
	state.SharedHeads = heads
	return state, nil
}

// Reset clears all ephemeral state.
func (s *State) Reset() {
	s.LastSentHeads = nil
	s.TheirHeads = nil
	s.TheirNeed = nil
	s.TheirHave = nil
	s.SentHashes = make(map[types.ChangeHash]bool)
	s.InFlight = false
	s.HaveResponded = false
	s.theirHeadsSet = false
	s.theirNeedSet = false
	s.theirHaveSet = false
}

// SetTheirState updates the state with information from a received message.
func (s *State) SetTheirState(heads, need []types.ChangeHash, have []Have) {
	s.TheirHeads = heads
	s.TheirNeed = need
	s.TheirHave = have
	s.theirHeadsSet = true
	s.theirNeedSet = true
	s.theirHaveSet = true
}

// TheirHeadsKnown returns true if we've received their heads.
func (s *State) TheirHeadsKnown() bool { return s.theirHeadsSet }

// TheirHaveKnown returns true if we've received their have info.
func (s *State) TheirHaveKnown() bool { return s.theirHaveSet }
