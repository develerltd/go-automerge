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

// deltaSeek seeks to the given index within a delta-encoded slab.
func deltaSeek(slab *Slab, index int) (*Run[int64], deltaCursorState) {
	state := deltaCursorState{abs: slab.Abs()}
	if index == 0 {
		return nil, state
	}
	data := slab.Bytes()
	for {
		run, err := deltaNext(&state, data)
		if err != nil || run == nil {
			panic("deltaSeek: index out of bounds")
		}
		if state.rle.index >= index {
			return run, state
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
func deltaSplice(slab *Slab, index, del int, values []int64, slabSize int) SpliceResult {
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
func deltaDecodeAllAbsolute(slab *Slab) []*int64 {
	state := deltaCursorState{abs: slab.Abs()}
	data := slab.Bytes()
	result := make([]*int64, 0, slab.Len())

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
				result = append(result, &v)
			}
		}
	}
	return result
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
