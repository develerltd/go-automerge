package opset

import "github.com/develerltd/go-automerge/internal/types"

// Clock represents a point in the document's history as a vector of per-actor
// maximum operation counters. An operation is "covered" by a clock if the
// clock's counter for that actor is >= the operation's counter.
type Clock struct {
	// maxCounter[actorIdx] = highest op counter for that actor at this point in time.
	maxCounter []uint64
}

// NewClock creates a clock from per-actor max counters.
func NewClock(maxCounters []uint64) Clock {
	return Clock{maxCounter: maxCounters}
}

// Covers returns true if this clock includes the given operation.
func (c Clock) Covers(id types.OpId) bool {
	if int(id.ActorIdx) >= len(c.maxCounter) {
		return false
	}
	return c.maxCounter[id.ActorIdx] >= id.Counter
}

// IsEmpty returns true if the clock has no entries.
func (c Clock) IsEmpty() bool {
	return len(c.maxCounter) == 0
}
