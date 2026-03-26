package automerge

import (
	"fmt"

	"github.com/develerltd/go-automerge/internal/columnar"
	"github.com/develerltd/go-automerge/internal/opset"
	"github.com/develerltd/go-automerge/internal/storage"
	gosync "github.com/develerltd/go-automerge/internal/sync"
	"github.com/develerltd/go-automerge/internal/types"
)

// SyncState tracks the sync state between two peers.
// Re-export from internal.
type SyncState = gosync.State

// NewSyncState creates a new sync state for a fresh sync session.
func NewSyncState() *SyncState {
	return gosync.NewState()
}

// GenerateSyncMessage produces the next sync message to send to the peer,
// or nil if no message is needed (already in sync).
func (d *Doc) GenerateSyncMessage(state *SyncState) *gosync.Message {
	// Auto-commit pending ops
	if len(d.pendingOps) > 0 {
		d.Commit("", 0)
	}

	ourHeads := d.heads

	// What we need from them
	var ourNeed []types.ChangeHash
	if state.TheirHeadsKnown() {
		ourNeed = d.getMissingDeps(state.TheirHeads)
	}

	// Build our "have" (bloom filter of what we have since shared_heads)
	var ourHave []gosync.Have
	if len(ourNeed) == 0 {
		// Only advertise what we have if we don't have unmet dependencies
		changesSinceShared := d.getChangeHashesSince(state.SharedHeads)
		bloom := gosync.BloomFromHashes(changesSinceShared)
		ourHave = []gosync.Have{{
			LastSync: state.SharedHeads,
			Bloom:    bloom,
		}}
	}

	// Determine what changes to send
	var changes [][]byte
	if state.TheirHaveKnown() {
		hashes := d.getHashesToSend(state.TheirHave, state.TheirNeed, state.SentHashes)
		for _, h := range hashes {
			if data, ok := d.changeData[h]; ok {
				changes = append(changes, data)
				state.SentHashes[h] = true
			}
		}
	}

	// Check if we should send a message
	if state.HaveResponded && headsEqual(ourHeads, state.LastSentHeads) {
		if len(changes) == 0 {
			if state.TheirHeadsKnown() && headsEqual(ourHeads, state.TheirHeads) {
				return nil // Already in sync
			}
			if state.InFlight {
				return nil // Waiting for ack of previous message
			}
			if len(ourNeed) == 0 && state.TheirHaveKnown() {
				return nil // Nothing to exchange
			}
		}
	}

	state.InFlight = true
	state.HaveResponded = true
	state.LastSentHeads = make([]types.ChangeHash, len(ourHeads))
	copy(state.LastSentHeads, ourHeads)

	return &gosync.Message{
		Heads:   ourHeads,
		Need:    ourNeed,
		Have:    ourHave,
		Changes: changes,
		Version: gosync.MessageV1Type,
	}
}

// ReceiveSyncMessage processes an incoming sync message from a peer.
func (d *Doc) ReceiveSyncMessage(state *SyncState, msg *gosync.Message) error {
	state.InFlight = false

	// Apply incoming changes
	if len(msg.Changes) > 0 {
		for _, changeBytes := range msg.Changes {
			if err := d.applyChangeBytes(changeBytes); err != nil {
				return fmt.Errorf("applying change: %w", err)
			}
		}
	}

	// Update sync state with their info
	state.SetTheirState(msg.Heads, msg.Need, msg.Have)

	// Update shared heads
	if d.hasAllChanges(msg.Heads) {
		state.SharedHeads = make([]types.ChangeHash, len(msg.Heads))
		copy(state.SharedHeads, msg.Heads)
	} else {
		// Merge known heads into shared_heads
		for _, h := range msg.Heads {
			if d.hasChangeHash(h) {
				found := false
				for _, sh := range state.SharedHeads {
					if sh == h {
						found = true
						break
					}
				}
				if !found {
					state.SharedHeads = append(state.SharedHeads, h)
				}
			}
		}
	}

	return nil
}

// hasChangeHash checks if we have a change with the given hash.
func (d *Doc) hasChangeHash(hash types.ChangeHash) bool {
	if _, ok := d.changeData[hash]; ok {
		return true
	}
	for _, cr := range d.changeRecords {
		if cr.Hash == hash {
			return true
		}
	}
	return false
}

// hasAllChanges checks if we have all the given change hashes.
func (d *Doc) hasAllChanges(hashes []types.ChangeHash) bool {
	for _, h := range hashes {
		if !d.hasChangeHash(h) {
			return false
		}
	}
	return true
}

// getMissingDeps returns hashes from the given set that we don't have.
func (d *Doc) getMissingDeps(heads []types.ChangeHash) []types.ChangeHash {
	var missing []types.ChangeHash
	for _, h := range heads {
		if !d.hasChangeHash(h) {
			missing = append(missing, h)
		}
	}
	return missing
}

// getChangeHashesSince returns all change hashes reachable from current heads
// but not from the given baseline heads.
func (d *Doc) getChangeHashesSince(baseline []types.ChangeHash) []types.ChangeHash {
	baselineSet := make(map[types.ChangeHash]bool)
	for _, h := range baseline {
		baselineSet[h] = true
	}

	// BFS from current heads backwards through deps
	visited := make(map[types.ChangeHash]bool)
	var result []types.ChangeHash
	queue := make([]types.ChangeHash, len(d.heads))
	copy(queue, d.heads)

	for len(queue) > 0 {
		h := queue[0]
		queue = queue[1:]

		if visited[h] || baselineSet[h] {
			continue
		}
		visited[h] = true
		result = append(result, h)

		// Find deps for this hash
		for _, cr := range d.changeRecords {
			if cr.Hash == h {
				for _, dep := range cr.DepHashes {
					if !visited[dep] && !baselineSet[dep] {
						queue = append(queue, dep)
					}
				}
				break
			}
		}
	}

	return result
}

// getHashesToSend determines which change hashes to send based on what the peer has.
func (d *Doc) getHashesToSend(theirHave []gosync.Have, theirNeed []types.ChangeHash, sentHashes map[types.ChangeHash]bool) []types.ChangeHash {
	// Collect baseline from their last_sync
	var lastSyncHashes []types.ChangeHash
	for _, h := range theirHave {
		lastSyncHashes = append(lastSyncHashes, h.LastSync...)
	}

	// Get all our changes since their baseline
	changesSince := d.getChangeHashesSince(lastSyncHashes)

	// Find hashes NOT in any of their bloom filters (they don't have them)
	var hashesToSend []types.ChangeHash
	for _, h := range changesSince {
		if sentHashes[h] {
			continue
		}
		inBloom := false
		for _, have := range theirHave {
			if have.Bloom.ContainsHash(h) {
				inBloom = true
				break
			}
		}
		if !inBloom {
			hashesToSend = append(hashesToSend, h)
		}
	}

	// Also add anything they explicitly need
	for _, h := range theirNeed {
		if !sentHashes[h] && d.hasChangeHash(h) {
			found := false
			for _, existing := range hashesToSend {
				if existing == h {
					found = true
					break
				}
			}
			if !found {
				hashesToSend = append(hashesToSend, h)
			}
		}
	}

	// Add transitive dependents: if we're sending hash X, and hash Y depends on X,
	// and Y is not in their bloom, we should send Y too.
	depMap := make(map[types.ChangeHash][]types.ChangeHash) // dep -> dependents
	for _, cr := range d.changeRecords {
		for _, dep := range cr.DepHashes {
			depMap[dep] = append(depMap[dep], cr.Hash)
		}
	}

	sendSet := make(map[types.ChangeHash]bool)
	for _, h := range hashesToSend {
		sendSet[h] = true
	}

	stack := make([]types.ChangeHash, len(hashesToSend))
	copy(stack, hashesToSend)
	for len(stack) > 0 {
		h := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, dependent := range depMap[h] {
			if !sendSet[dependent] && !sentHashes[dependent] {
				sendSet[dependent] = true
				hashesToSend = append(hashesToSend, dependent)
				stack = append(stack, dependent)
			}
		}
	}

	return hashesToSend
}

// applyChangeBytes parses and applies a raw change chunk to the document.
func (d *Doc) applyChangeBytes(data []byte) error {
	// Compute hash
	hash := storage.ComputeHash(storage.ChunkTypeChange, data)

	// Check if we already have this change
	changeHash := types.ChangeHash(hash)
	if d.hasChangeHash(changeHash) {
		return nil // already have it
	}

	// Parse the change
	parsed, err := storage.ParseChangeChunk(data, hash)
	if err != nil {
		return fmt.Errorf("parsing change: %w", err)
	}

	// Ensure actor is in our table
	actorIdx := d.ensureActorInTable(ActorId(parsed.Actor))
	localActors := make([]uint32, 1+len(parsed.OtherActors))
	localActors[0] = actorIdx
	for i, other := range parsed.OtherActors {
		localActors[i+1] = d.ensureActorInTable(ActorId(other))
	}

	// Decode and apply ops
	changeOps, err := opset.DecodeChangeOps(
		columnar.RawColumns(parsed.OpColumns),
		types.ActorId(parsed.Actor),
		nil,
	)
	if err != nil {
		return fmt.Errorf("decoding change ops: %w", err)
	}

	for i := range changeOps {
		op := &changeOps[i]
		op.ID.ActorIdx = mapActorIdx(op.ID.ActorIdx, localActors)
		if !op.Obj.IsRoot() {
			op.Obj.ActorIdx = mapActorIdx(op.Obj.ActorIdx, localActors)
		}
		if op.Key.Kind == opset.KeySeq && !op.Key.ElemID.IsZero() {
			op.Key.ElemID.ActorIdx = mapActorIdx(op.Key.ElemID.ActorIdx, localActors)
		}
		for j := range op.Pred {
			op.Pred[j].ActorIdx = mapActorIdx(op.Pred[j].ActorIdx, localActors)
		}
		// Update predecessor ops' successor lists directly via columns
		for _, predId := range op.Pred {
			d.opSet.UpdateSucc(predId, op.ID)
		}
		d.opSet.AddOp(*op)
	}

	d.opSet.SortAndReindex()

	// Compute maxOp
	maxOp := parsed.StartOp
	for _, op := range changeOps {
		if op.ID.Counter > maxOp {
			maxOp = op.ID.Counter
		}
	}

	// Store change record
	depHashes := make([]types.ChangeHash, len(parsed.Deps))
	for i, dep := range parsed.Deps {
		depHashes[i] = types.ChangeHash(dep)
	}
	d.changeRecords = append(d.changeRecords, opset.ChangeRecord{
		Hash:      changeHash,
		ActorIdx:  actorIdx,
		Seq:       parsed.Seq,
		MaxOp:     maxOp,
		Time:      parsed.Timestamp,
		Message:   parsed.Message,
		DepHashes: depHashes,
	})

	// Store raw data
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	d.changeData[changeHash] = dataCopy

	// Update heads: remove deps that are satisfied, add new hash
	d.updateHeadsAfterChange(changeHash, depHashes)

	// Update nextOp
	if d.opSet.MaxOp >= d.nextOp {
		d.nextOp = d.opSet.MaxOp + 1
	}

	// Invalidate saved change columns
	d.savedChangeCols = nil

	return nil
}

// updateHeadsAfterChange updates document heads after applying a change.
func (d *Doc) updateHeadsAfterChange(newHash types.ChangeHash, deps []types.ChangeHash) {
	depSet := make(map[types.ChangeHash]bool)
	for _, dep := range deps {
		depSet[dep] = true
	}

	var newHeads []types.ChangeHash
	for _, h := range d.heads {
		if !depSet[h] {
			newHeads = append(newHeads, h)
		}
	}

	// Add new hash if not already present
	found := false
	for _, h := range newHeads {
		if h == newHash {
			found = true
			break
		}
	}
	if !found {
		newHeads = append(newHeads, newHash)
	}

	d.heads = newHeads
}

func headsEqual(a, b []types.ChangeHash) bool {
	if len(a) != len(b) {
		return false
	}
	aSet := make(map[types.ChangeHash]bool, len(a))
	for _, h := range a {
		aSet[h] = true
	}
	for _, h := range b {
		if !aSet[h] {
			return false
		}
	}
	return true
}
