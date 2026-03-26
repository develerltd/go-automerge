package hexane

import "sync"

// accSlicePool pools []Acc buffers used by ColumnDataIter.decodeSlab.
// Typical slab size is 64 items, so we pool slices with capacity 65.
var accSlicePool = sync.Pool{
	New: func() any {
		s := make([]Acc, 0, 65)
		return &s
	},
}

// ColumnDataIter is a forward iterator over a ColumnData column.
//
// It decodes one slab at a time, caching decoded values for efficient sequential access.
// Supports O(log n) jumps via AdvanceBy/AdvanceTo and O(1) per-item access.
type ColumnDataIter[T any] struct {
	col     *ColumnData[T]
	pos     int // current position (next item to yield)
	max     int // exclusive upper bound
	counter int // mutation counter at creation time

	// Slab tracking
	slabIdx    int // index of current slab in span tree
	preSlabAcc Acc // cumulative acc of all slabs before current one
	preSlabPos int // cumulative item count before current slab

	// Current slab decoded state
	currentSlab *Slab
	decoded     []*T  // decoded values from current slab
	decodedAcc  []Acc // decodedAcc[i] = cumulative acc of decoded[0..i)
	decIdx      int   // current index within decoded
}

// newIter creates a ColumnDataIter positioned at pos with upper bound max.
func (c *ColumnData[T]) newIter(pos, max int) *ColumnDataIter[T] {
	it := &ColumnDataIter[T]{
		col:     c,
		pos:     pos,
		max:     max,
		counter: c.counter,
	}

	if c.slabs.IsEmpty() || c.len == 0 || pos >= max {
		return it
	}

	// Find slab containing pos
	cursor := c.slabs.GetWhereOrLast(func(accW, nextW SlabWeight) bool {
		return pos < accW.Pos+nextW.Pos
	})
	if cursor == nil {
		return it
	}

	it.slabIdx = cursor.Index + 1
	it.preSlabAcc = cursor.Weight.Acc
	it.preSlabPos = cursor.Weight.Pos
	it.currentSlab = cursor.Element
	it.decodeSlab()

	// Advance within slab to reach pos
	offset := pos - it.preSlabPos
	if offset > len(it.decoded) {
		offset = len(it.decoded)
	}
	it.decIdx = offset

	return it
}

// decodeSlab decodes the current slab's values and precomputes cumulative acc.
// Uses sync.Pool for the []Acc buffer to reduce allocations.
func (it *ColumnDataIter[T]) decodeSlab() {
	// Return previous acc buffer to pool
	if it.decodedAcc != nil {
		buf := it.decodedAcc[:0]
		accSlicePool.Put(&buf)
		it.decodedAcc = nil
	}

	if it.currentSlab == nil {
		it.decoded = nil
		return
	}
	it.decoded = it.col.ops.DecodeAll(it.currentSlab)
	n := len(it.decoded) + 1

	// Get acc buffer from pool
	bufPtr := accSlicePool.Get().(*[]Acc)
	buf := *bufPtr
	if cap(buf) >= n {
		it.decodedAcc = buf[:n]
	} else {
		it.decodedAcc = make([]Acc, n)
	}
	it.decodedAcc[0] = Acc{}

	packer := it.col.ops.Packer
	for i, v := range it.decoded {
		if v != nil {
			agg := packer.ItemAgg(*v)
			it.decodedAcc[i+1] = it.decodedAcc[i].AddAgg(agg)
		} else {
			it.decodedAcc[i+1] = it.decodedAcc[i]
		}
	}
	it.decIdx = 0
}

// loadNextSlab advances to the next slab. Returns false if no more slabs.
func (it *ColumnDataIter[T]) loadNextSlab() bool {
	if it.currentSlab != nil {
		it.preSlabAcc.AddAssign(it.currentSlab.Acc())
		it.preSlabPos += it.currentSlab.Len()
	}
	slab := it.col.slabs.Get(it.slabIdx)
	if slab == nil {
		it.currentSlab = nil
		it.decoded = nil
		it.decodedAcc = nil
		return false
	}
	it.slabIdx++
	it.currentSlab = slab
	it.decodeSlab()
	return true
}

// --- Basic access ---

// Pos returns the current position (index of the next item to be yielded).
func (it *ColumnDataIter[T]) Pos() int {
	if it.pos > it.max {
		return it.max
	}
	return it.pos
}

// EndPos returns the exclusive upper bound of the iteration range.
func (it *ColumnDataIter[T]) EndPos() int { return it.max }

// SetMax overrides the upper bound.
func (it *ColumnDataIter[T]) SetMax(max int) { it.max = max }

// Next returns the next value (nil for null) and true, or nil, false if exhausted.
func (it *ColumnDataIter[T]) Next() (*T, bool) {
	if it.pos >= it.max {
		return nil, false
	}
	// Ensure we have a decoded slab
	for it.decIdx >= len(it.decoded) {
		if !it.loadNextSlab() {
			return nil, false
		}
	}
	val := it.decoded[it.decIdx]
	it.decIdx++
	it.pos++
	return val, true
}

// NextRun returns the next run of equal values, advancing pos by the run's count.
// Returns nil when exhausted.
func (it *ColumnDataIter[T]) NextRun() *Run[T] {
	if it.pos >= it.max {
		return nil
	}
	// Ensure we have decoded values
	for it.decIdx >= len(it.decoded) {
		if !it.loadNextSlab() {
			return nil
		}
	}

	val := it.decoded[it.decIdx]
	count := 1
	limit := len(it.decoded) - it.decIdx
	if it.pos+limit > it.max {
		limit = it.max - it.pos
	}

	eq := it.col.ops.EqualValue
	for count < limit {
		if !eq(val, it.decoded[it.decIdx+count]) {
			break
		}
		count++
	}

	it.decIdx += count
	it.pos += count
	return &Run[T]{Count: count, Value: val}
}

// RunCount returns the number of equal values remaining in the current run
// (starting from current position). This is a peek — it doesn't advance.
func (it *ColumnDataIter[T]) RunCount() int {
	if it.pos >= it.max || it.decIdx >= len(it.decoded) {
		return 0
	}
	val := it.decoded[it.decIdx]
	count := 1
	limit := len(it.decoded) - it.decIdx
	if it.pos+limit > it.max {
		limit = it.max - it.pos
	}
	eq := it.col.ops.EqualValue
	for count < limit {
		if !eq(val, it.decoded[it.decIdx+count]) {
			break
		}
		count++
	}
	return count
}

// --- Navigation ---

// AdvanceBy advances the iterator by amount items. O(log n) for large jumps.
func (it *ColumnDataIter[T]) AdvanceBy(amount int) {
	if amount <= 0 {
		return
	}
	target := it.pos + amount
	if target > it.max {
		target = it.max
	}
	if target == it.pos {
		return
	}

	advance := target - it.pos
	remaining := len(it.decoded) - it.decIdx
	if advance <= remaining {
		it.decIdx += advance
		it.pos = target
		return
	}

	// Need to jump via span tree
	it.seekToPos(target)
}

// AdvanceTo advances the iterator to absolute position target. O(log n).
// Panics if target < Pos().
func (it *ColumnDataIter[T]) AdvanceTo(target int) {
	if target < it.pos {
		panic("ColumnDataIter.AdvanceTo: target before current position")
	}
	it.AdvanceBy(target - it.pos)
}

// seekToPos repositions the iterator to the given absolute position.
func (it *ColumnDataIter[T]) seekToPos(pos int) {
	if pos >= it.max {
		it.pos = it.max
		it.decoded = nil
		it.decodedAcc = nil
		it.currentSlab = nil
		return
	}

	cursor := it.col.slabs.GetWhereOrLast(func(accW, nextW SlabWeight) bool {
		return pos < accW.Pos+nextW.Pos
	})
	if cursor == nil {
		it.pos = pos
		return
	}

	it.slabIdx = cursor.Index + 1
	it.preSlabAcc = cursor.Weight.Acc
	it.preSlabPos = cursor.Weight.Pos
	it.currentSlab = cursor.Element
	it.decodeSlab()

	offset := pos - it.preSlabPos
	if offset > len(it.decoded) {
		offset = len(it.decoded)
	}
	it.decIdx = offset
	it.pos = pos
}

// ShiftNext moves the iterator window to [start, end) and returns the item at start.
// The iterator must already be at or before start.
func (it *ColumnDataIter[T]) ShiftNext(start, end int) (*T, bool) {
	if start < it.pos {
		panic("ColumnDataIter.ShiftNext: start before current position")
	}
	it.max = end
	if start > it.pos {
		it.AdvanceTo(start)
	}
	return it.Next()
}

// --- Accumulator ---

// CalculateAcc returns the cumulative Acc for all items before the current position.
func (it *ColumnDataIter[T]) CalculateAcc() Acc {
	if it.decodedAcc == nil || it.decIdx > len(it.decodedAcc)-1 {
		return it.preSlabAcc
	}
	return it.preSlabAcc.Add(it.decodedAcc[it.decIdx])
}

// AdvanceAccBy advances the iterator until the cumulative Acc has grown by at least n.
// Returns the number of items consumed.
func (it *ColumnDataIter[T]) AdvanceAccBy(n Acc) int {
	startPos := it.pos
	target := it.CalculateAcc().Add(n)
	packer := it.col.ops.Packer

	for it.pos < it.max {
		// Try to advance within current slab
		for it.decIdx < len(it.decoded) && it.pos < it.max {
			currentAcc := it.preSlabAcc.Add(it.decodedAcc[it.decIdx])
			if !currentAcc.Less(target) {
				return it.pos - startPos
			}
			if it.decoded[it.decIdx] != nil {
				agg := packer.ItemAgg(*it.decoded[it.decIdx])
				_ = agg // acc check is at the start of loop
			}
			it.decIdx++
			it.pos++
		}
		// Check if we've reached target
		currentAcc := it.CalculateAcc()
		if !currentAcc.Less(target) {
			return it.pos - startPos
		}
		// Move to next slab
		if !it.loadNextSlab() {
			break
		}
	}
	return it.pos - startPos
}

// --- Suspend / Resume ---

// ColumnDataIterState captures the state of a ColumnDataIter for later resumption.
type ColumnDataIterState struct {
	Counter    int
	Pos        int
	Max        int
	SlabIdx    int
	PreSlabAcc Acc
	PreSlabPos int
	DecIdx     int
}

// Suspend captures the current iterator position.
func (it *ColumnDataIter[T]) Suspend() ColumnDataIterState {
	return ColumnDataIterState{
		Counter:    it.counter,
		Pos:        it.pos,
		Max:        it.max,
		SlabIdx:    it.slabIdx,
		PreSlabAcc: it.preSlabAcc,
		PreSlabPos: it.preSlabPos,
		DecIdx:     it.decIdx,
	}
}

// Resume restores the iterator to a previously suspended state.
// Returns an error if the column was mutated since suspension.
func ResumeColumnDataIter[T any](col *ColumnData[T], state ColumnDataIterState) (*ColumnDataIter[T], error) {
	if col.counter != state.Counter {
		return nil, NewPackError("column mutated since iterator suspension")
	}

	it := &ColumnDataIter[T]{
		col:        col,
		pos:        state.Pos,
		max:        state.Max,
		counter:    state.Counter,
		slabIdx:    state.SlabIdx,
		preSlabAcc: state.PreSlabAcc,
		preSlabPos: state.PreSlabPos,
	}

	// Reload the slab at the saved position
	if state.SlabIdx > 0 {
		slab := col.slabs.Get(state.SlabIdx - 1)
		if slab != nil {
			it.currentSlab = slab
			it.decodeSlab()
			it.decIdx = state.DecIdx
		}
	}

	return it, nil
}

// --- SeekToValue ---

// SeekToValue finds the contiguous range of value within [rangeStart, rangeEnd).
// Values in that range must be sorted. Returns (start, end) of the matching range,
// or an empty range if not found.
// After returning, the iterator is positioned at start of the matching range.
//
// When CompareValue is available (ordered types), uses O(log n) binary search on
// the slab tree to skip to the right neighborhood before linear scanning.
func (it *ColumnDataIter[T]) SeekToValue(value *T, rangeStart, rangeEnd int) (int, int) {
	if rangeEnd > it.max {
		rangeEnd = it.max
	}

	if rangeStart > it.pos {
		it.AdvanceTo(rangeStart)
	}

	// Binary search optimization: skip slabs that can't contain the target
	cmp := it.col.ops.CompareValue
	if cmp != nil {
		if slabIdx, ok := it.binarySearchForSlab(value, rangeEnd); ok {
			it.resetToSlabIndex(slabIdx)
		}
	}

	// Linear scan for exact boundaries
	foundStart := -1
	foundEnd := it.pos

	for it.pos < rangeEnd {
		if it.decIdx >= len(it.decoded) {
			if !it.loadNextSlab() {
				break
			}
		}

		for it.decIdx < len(it.decoded) && it.pos < rangeEnd {
			v := it.decoded[it.decIdx]

			if cmp != nil {
				c := cmp(v, value)
				if c == 0 {
					if foundStart < 0 {
						foundStart = it.pos
					}
					foundEnd = it.pos + 1
				} else if c > 0 {
					// Past the matching range (sorted)
					goto done
				}
			} else {
				if it.col.ops.EqualValue(v, value) {
					if foundStart < 0 {
						foundStart = it.pos
					}
					foundEnd = it.pos + 1
				} else if foundStart >= 0 {
					goto done
				}
			}

			it.decIdx++
			it.pos++
		}
	}

done:
	if foundStart < 0 {
		p := it.pos
		return p, p
	}

	if it.pos != foundStart {
		it.seekToPos(foundStart)
	}

	return foundStart, foundEnd
}

// binarySearchForSlab performs a binary search on the slab tree to find the last
// slab whose first value is less than the target. Returns the slab index to reset
// to, or (0, false) if the target is in the current slab.
//
// This mirrors upstream Rust hexane's binary_search_for: it uses the first decoded
// value of each slab as the search key, giving O(log n) slab-level seeking.
func (it *ColumnDataIter[T]) binarySearchForSlab(target *T, maxPos int) (int, bool) {
	col := it.col
	cmp := col.ops.CompareValue

	originalSlabIdx := it.slabIdx - 1
	if originalSlabIdx < 0 {
		originalSlabIdx = 0
	}

	// Use DecodeFirst when available (avoids allocating []*T slice)
	decodeFirst := col.ops.DecodeFirst
	if decodeFirst == nil {
		// Fallback: decode all and take first
		decodeFirst = func(slab *Slab) *T {
			decoded := col.ops.DecodeAll(slab)
			if len(decoded) == 0 {
				return nil
			}
			return decoded[0]
		}
	}

	// Check next slab's first value
	nextSlab := col.slabs.Get(it.slabIdx)
	if nextSlab == nil || nextSlab.Len() == 0 {
		return 0, false
	}
	nextFirst := decodeFirst(nextSlab)

	nextCmp := cmp(nextFirst, target)
	if nextCmp > 0 {
		return 0, false // target is before next slab
	}
	if nextCmp == 0 {
		return 0, false // could be in current slab
	}

	// Find the slab containing maxPos
	maxSlabCursor := col.slabs.GetWhereOrLast(func(accW, nextW SlabWeight) bool {
		return maxPos < accW.Pos+nextW.Pos
	})
	if maxSlabCursor == nil {
		return 0, false
	}

	start := originalSlabIdx
	end := maxSlabCursor.Index
	mid := (start + end + 1) / 2

	for start < mid && mid < end {
		midSlab := col.slabs.Get(mid)
		if midSlab == nil {
			break
		}
		midFirst := decodeFirst(midSlab)
		if midFirst != nil && cmp(midFirst, target) < 0 {
			start = mid
		} else {
			end = mid
		}
		mid = (start + end + 1) / 2
	}

	if start != originalSlabIdx {
		return start, true
	}
	return 0, false
}

// resetToSlabIndex repositions the iterator to the beginning of the given slab.
func (it *ColumnDataIter[T]) resetToSlabIndex(slabIdx int) {
	cursor := it.col.slabs.GetCursor(slabIdx)
	if cursor == nil {
		return
	}
	it.slabIdx = cursor.Index + 1
	it.preSlabAcc = cursor.Weight.Acc
	it.preSlabPos = cursor.Weight.Pos
	it.currentSlab = cursor.Element
	it.decodeSlab()
	it.pos = it.preSlabPos
}

// --- Adapter iterators ---

// ColGroupItem bundles a value with its position and pre-item accumulator.
type ColGroupItem[T any] struct {
	Acc  Acc // accumulator immediately before this item
	Pos  int // zero-based index in the column
	Item *T  // the value (nil for null)
}

// NextAcc returns the accumulator after this item.
func (g ColGroupItem[T]) NextAcc(packer Packer[T]) Acc {
	if g.Item != nil {
		return g.Acc.AddAgg(packer.ItemAgg(*g.Item))
	}
	return g.Acc
}

// ColGroupIter wraps a ColumnDataIter and emits ColGroupItem values.
type ColGroupIter[T any] struct {
	Iter *ColumnDataIter[T]
}

// WithAcc wraps the iterator in a ColGroupIter.
func (it *ColumnDataIter[T]) WithAcc() *ColGroupIter[T] {
	return &ColGroupIter[T]{Iter: it}
}

// Next returns the next ColGroupItem, or nil, false if exhausted.
func (g *ColGroupIter[T]) Next() (*ColGroupItem[T], bool) {
	acc := g.Iter.CalculateAcc()
	pos := g.Iter.pos
	item, ok := g.Iter.Next()
	if !ok {
		return nil, false
	}
	return &ColGroupItem[T]{Acc: acc, Pos: pos, Item: item}, true
}

// AdvanceBy advances the underlying iterator by amount items.
func (g *ColGroupIter[T]) AdvanceBy(amount int) {
	g.Iter.AdvanceBy(amount)
}

// ShiftAcc advances by accumulator amount n and returns the next item.
func (g *ColGroupIter[T]) ShiftAcc(n Acc) (*ColGroupItem[T], bool) {
	g.Iter.AdvanceAccBy(n)
	return g.Next()
}

// RunCount returns the items remaining in the current run.
func (g *ColGroupIter[T]) RunCount() int {
	return g.Iter.RunCount()
}

// Acc returns the current pre-item accumulator.
func (g *ColGroupIter[T]) Acc() Acc {
	return g.Iter.CalculateAcc()
}

// ColAccIter wraps a ColumnDataIter and emits only the Acc value after each item.
type ColAccIter[T any] struct {
	Iter *ColumnDataIter[T]
}

// AsAcc wraps the iterator in a ColAccIter.
func (it *ColumnDataIter[T]) AsAcc() *ColAccIter[T] {
	return &ColAccIter[T]{Iter: it}
}

// Next returns the Acc after consuming the next item, or Acc{}, false if exhausted.
func (a *ColAccIter[T]) Next() (Acc, bool) {
	_, ok := a.Iter.Next()
	if !ok {
		return Acc{}, false
	}
	return a.Iter.CalculateAcc(), true
}

// ShiftNext moves the window and returns the Acc.
func (a *ColAccIter[T]) ShiftNext(start, end int) (Acc, bool) {
	_, ok := a.Iter.ShiftNext(start, end)
	if !ok {
		return Acc{}, false
	}
	return a.Iter.CalculateAcc(), true
}
