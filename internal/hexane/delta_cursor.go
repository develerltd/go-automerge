package hexane

// deltaCursorState wraps rleCursorState with absolute value tracking.
type deltaCursorState struct {
	abs int64
	min Agg
	max Agg
	rle rleCursorState
}

// deltaTryNext decodes the next run from a delta-encoded slab.
// The returned Run values are deltas, not absolute values.
func deltaTryNext(state *deltaCursorState, data []byte) (*Run[int64], error) {
	run, err := rleTryNext(&state.rle, data, Int64Packer{})
	if err != nil || run == nil {
		return nil, err
	}

	delta := RunDelta(run)
	abs := state.abs + delta

	// Track min/max of absolute values
	var firstStep int64
	if run.Value != nil {
		firstStep = state.abs + *run.Value
	} else {
		firstStep = state.abs
	}
	minVal := abs
	if firstStep < minVal {
		minVal = firstStep
	}
	maxVal := abs
	if firstStep > maxVal {
		maxVal = firstStep
	}

	state.min = state.min.Minimize(AggFromInt64(minVal))
	state.max = state.max.Maximize(AggFromInt64(maxVal))
	state.abs = abs

	return run, nil
}

// deltaNext decodes the next non-empty delta run.
func deltaNext(state *deltaCursorState, data []byte) (*Run[int64], error) {
	for {
		run, err := deltaTryNext(state, data)
		if err != nil || run == nil {
			return nil, err
		}
		if run.Count > 0 {
			return run, nil
		}
	}
}

// deltaLoad parses raw delta-encoded bytes into slabs.
func deltaLoad(data []byte, slabSize int) ([]Slab, int, error) {
	var state deltaCursorState
	writer := NewSlabWriter[int64](Int64Packer{}, slabSize, true)
	var lastOffset int

	for {
		run, err := deltaTryNext(&state, data)
		if err != nil {
			return nil, 0, err
		}
		if run == nil {
			break
		}

		if state.rle.offset-lastOffset >= slabSize {
			if lastOffset < state.rle.lastOffset {
				writer.Copy(data, lastOffset, state.rle.lastOffset, 0, state.rle.index-run.Count, Acc{}, nil)
			}
			writer.SetAbs(state.abs)
			writer.ManualSlabBreak()
			lastOffset = state.rle.lastOffset
		}
	}

	// Copy remaining
	if lastOffset < state.rle.offset {
		writer.Copy(data, lastOffset, state.rle.offset, 0, state.rle.index, Acc{}, nil)
	}

	slabs := writer.Finish()
	return slabs, state.rle.index, nil
}

// deltaComputeMinMax computes min/max metadata for delta-encoded slabs.
func deltaComputeMinMax(slabs []Slab) {
	for i := range slabs {
		s := &slabs[i]
		state := deltaCursorState{abs: s.Abs()}
		data := s.Bytes()
		for {
			run, err := deltaNext(&state, data)
			if err != nil || run == nil {
				break
			}
		}
		s.SetMinMax(state.min, state.max)
	}
}

// deltaSplice performs a splice within a delta-encoded slab.
// Values are absolute (not deltas) — the splice handles the delta conversion.
// deltaSpliceInsertOne handles inserting a single absolute value with no deletion.
// Works at the run level (O(num_runs)) instead of decoding all items (O(num_items)).
func deltaSpliceInsertOne(slab *Slab, index int, absValue int64, slabSize int) SpliceResult {
	// Collect delta runs and track absolute positions
	var cstate deltaCursorState
	cstate.abs = slab.Abs()
	data := slab.Bytes()

	type runEntry struct {
		run       Run[int64]
		absBefore int64 // absolute value before this run
	}
	var entries []runEntry
	pos := 0
	abs := slab.Abs()
	insertIdx := -1
	insertOff := 0

	for {
		entryAbs := abs
		run, err := deltaNext(&cstate, data)
		if err != nil || run == nil {
			break
		}
		if insertIdx == -1 && pos+run.Count > index {
			insertIdx = len(entries)
			insertOff = index - pos
		}
		entries = append(entries, runEntry{run: *run, absBefore: entryAbs})
		abs += RunDelta(run)
		pos += run.Count
	}

	if insertIdx == -1 {
		insertIdx = len(entries)
		insertOff = 0
	}

	// Re-encode from runs with new value inserted
	encState := NewDeltaEncState()
	encState.Abs = slab.Abs()
	writer := NewSlabWriter[int64](Int64Packer{}, slabSize, false)
	writer.SetAbs(slab.Abs())
	writer.SetInitAbs(slab.Abs())

	for i, e := range entries {
		if i != insertIdx {
			encState.AppendChunk(writer, e.run)
			continue
		}

		// Compute absolute at insert position within this run
		var absAtInsert int64
		if e.run.Value != nil {
			absAtInsert = e.absBefore + int64(insertOff)*(*e.run.Value)
		} else {
			absAtInsert = e.absBefore
		}
		newDelta := absValue - absAtInsert

		// Emit prefix
		if insertOff > 0 {
			prefix := e.run
			prefix.Count = insertOff
			encState.AppendChunk(writer, prefix)
		}

		// Emit new value
		encState.AppendChunk(writer, Run[int64]{Count: 1, Value: &newDelta})

		// Emit suffix
		remaining := e.run.Count - insertOff
		if remaining > 0 {
			var absNext int64
			if e.run.Value != nil {
				absNext = e.absBefore + int64(insertOff+1)*(*e.run.Value)
			} else {
				absNext = e.absBefore
			}
			suffixDelta := absNext - absValue
			encState.AppendChunk(writer, Run[int64]{Count: 1, Value: &suffixDelta})
			if remaining > 1 {
				suffix := e.run
				suffix.Count = remaining - 1
				encState.AppendChunk(writer, suffix)
			}
		}
	}

	if insertIdx == len(entries) {
		// Append at end
		newDelta := absValue - abs
		encState.AppendChunk(writer, Run[int64]{Count: 1, Value: &newDelta})
	}

	encState.Flush(writer)
	slabs := writer.Finish()
	if len(slabs) == 0 {
		slabs = []Slab{{}}
	}
	deltaComputeMinMax(slabs)

	return SpliceResult{
		Add:   1,
		Del:   0,
		Slabs: slabs,
	}
}

func deltaSplice(slab *Slab, index, del int, values []int64, slabSize int) SpliceResult {
	// Fast path: inserting a single value with no deletion
	if del == 0 && len(values) == 1 {
		return deltaSpliceInsertOne(slab, index, values[0], slabSize)
	}

	// Decode to absolute values
	decoded := deltaDecodeAllAbsolute(slab)

	overflow := 0
	actualDel := del
	if index+del > len(decoded) {
		overflow = index + del - len(decoded)
		actualDel = len(decoded) - index
	}

	// Apply splice (values are absolute)
	newLen := len(decoded) - actualDel + len(values)
	result := make([]*int64, 0, newLen)
	result = append(result, decoded[:index]...)
	for i := range values {
		v := values[i]
		result = append(result, &v)
	}
	result = append(result, decoded[index+actualDel:]...)

	// Re-encode using delta encoder
	state := NewDeltaEncState()
	writer := NewSlabWriter[int64](Int64Packer{}, slabSize, false)
	for _, v := range result {
		state.Append(writer, v)
	}
	state.Flush(writer)
	slabs := writer.Finish()
	if len(slabs) == 0 {
		slabs = []Slab{{}}
	}

	deltaComputeMinMax(slabs)

	return SpliceResult{
		Add:      len(values),
		Del:      actualDel,
		Overflow: overflow,
		Slabs:    slabs,
	}
}

// deltaDecodeAllAbsolute decodes all values from a delta slab as absolute values.
// Returns *int64 pointers (nil for null runs).
// Uses a batch backing array to reduce allocations from O(N) to O(1) per slab.
func deltaDecodeAllAbsolute(slab *Slab) []*int64 {
	state := deltaCursorState{abs: slab.Abs()}
	data := slab.Bytes()
	n := slab.Len()
	backing := make([]int64, 0, n)
	result := make([]*int64, 0, n)

	for {
		run, err := deltaNext(&state, data)
		if err != nil || run == nil {
			break
		}
		if run.Value == nil {
			for i := 0; i < run.Count; i++ {
				result = append(result, nil)
			}
		} else {
			// For a delta run with value=d and count=n:
			// The absolute values are: abs - d*(n-1), abs - d*(n-2), ..., abs
			// where abs is the cursor's abs AFTER processing this run.
			delta := *run.Value
			for i := 0; i < run.Count; i++ {
				v := state.abs - delta*int64(run.Count-1-i)
				backing = append(backing, v)
				result = append(result, &backing[len(backing)-1])
			}
		}
	}
	return result
}

// deltaDecodeFirst decodes only the first value from a delta slab as an absolute value.
func deltaDecodeFirst(slab *Slab) *int64 {
	state := deltaCursorState{abs: slab.Abs()}
	data := slab.Bytes()
	run, err := deltaNext(&state, data)
	if err != nil || run == nil {
		return nil
	}
	if run.Value == nil {
		return nil
	}
	// First value: abs - delta*(count-1)
	delta := *run.Value
	v := state.abs - delta*int64(run.Count-1)
	return &v
}

// deltaDecodeAt decodes the value at the given offset within a delta slab.
// Walks runs until the target offset, computing the absolute value.
func deltaDecodeAt(slab *Slab, offset int) *int64 {
	state := deltaCursorState{abs: slab.Abs()}
	data := slab.Bytes()
	pos := 0
	for {
		run, err := deltaNext(&state, data)
		if err != nil || run == nil {
			return nil
		}
		if pos+run.Count > offset {
			if run.Value == nil {
				return nil
			}
			delta := *run.Value
			idx := offset - pos
			v := state.abs - delta*int64(run.Count-1-idx)
			return &v
		}
		pos += run.Count
	}
}

// deltaEncode encodes absolute int64 values to bytes using delta encoding.
func deltaEncode(values []int64) []byte {
	state := NewDeltaEncState()
	writer := NewSlabWriter[int64](Int64Packer{}, 1<<30, true)
	for i := range values {
		state.Append(writer, &values[i])
	}
	state.Flush(writer)
	return writer.Write(nil)
}

// deltaInitEmpty creates an empty delta slab of given length.
func deltaInitEmpty(length int) Slab {
	return rleInitEmpty[int64](length, Int64Packer{})
}
