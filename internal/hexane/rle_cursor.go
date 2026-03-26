package hexane

import (
	"github.com/develerltd/go-automerge/internal/encoding"
)

// rleCursorState tracks the decode state for an RLE cursor within a slab.
type rleCursorState struct {
	offset     int
	lastOffset int
	index      int
	acc        Acc
	min        Agg
	max        Agg
	lit        *litRunState
}

// litRunState tracks position within a literal run.
type litRunState struct {
	offset int // byte offset where lit run values start
	index  int // current position within lit run (0-based, starts at 1 after first read)
	length int // total items in lit run
	acc    Acc // acc at start of lit run
}

func (l *litRunState) numLeft() int {
	if l.index > l.length {
		return 0
	}
	return l.length - l.index
}

// rleTryNext decodes the next run from slab data using the given packer.
// Returns nil when the slab is exhausted.
func rleTryNext[T any](state *rleCursorState, data []byte, packer Packer[T]) (*Run[T], error) {
	if state.offset >= len(data) {
		return nil, nil
	}

	remaining := data[state.offset:]

	// If we're in a literal run, read the next packed value
	if state.lit != nil && state.lit.numLeft() > 0 {
		value, n, err := packer.Unpack(remaining)
		if err != nil {
			return nil, err
		}
		agg := packer.ItemAgg(value)
		state.lit.index++
		if state.lit.index > state.lit.length {
			state.lit = nil
		}
		state.progress(1, n, agg)
		return &Run[T]{Count: 1, Value: &value}, nil
	}

	// Read the count header (sLEB128)
	count, countBytes, err := encoding.ReadSLEB128(remaining, 0)
	if err != nil {
		return nil, err
	}
	remaining = remaining[countBytes:]

	switch {
	case count > 0:
		// Positive count: value run
		value, valueBytes, err := packer.Unpack(remaining)
		if err != nil {
			return nil, err
		}
		agg := packer.ItemAgg(value)
		state.lit = nil
		state.progress(int(count), countBytes+valueBytes, agg)
		return &Run[T]{Count: int(count), Value: &value}, nil

	case count < 0:
		// Negative count: literal run
		litLen := int(-count)
		value, valueBytes, err := packer.Unpack(remaining)
		if err != nil {
			return nil, err
		}
		agg := packer.ItemAgg(value)
		state.lit = &litRunState{
			offset: state.offset + countBytes,
			index:  1,
			length: litLen,
			acc:    state.acc,
		}
		state.progress(1, countBytes+valueBytes, agg)
		return &Run[T]{Count: 1, Value: &value}, nil

	default:
		// Zero count header: null run
		nullCount, nullBytes, err := encoding.ReadULEB128(remaining, 0)
		if err != nil {
			return nil, err
		}
		state.lit = nil
		state.progress(int(nullCount), countBytes+nullBytes, Agg{})
		return &Run[T]{Count: int(nullCount), Value: nil}, nil
	}
}

func (state *rleCursorState) progress(count, bytes int, agg Agg) {
	state.lastOffset = state.offset
	state.offset += bytes
	state.index += count
	state.acc.AddAssign(agg.MulUint(count))
	state.min = state.min.Minimize(agg)
	state.max = state.max.Maximize(agg)
}

// rleNext decodes the next non-empty run, skipping zero-count runs.
func rleNext[T any](state *rleCursorState, data []byte, packer Packer[T]) (*Run[T], error) {
	for {
		run, err := rleTryNext(state, data, packer)
		if err != nil || run == nil {
			return nil, err
		}
		if run.Count > 0 {
			return run, nil
		}
	}
}

// rleLoad parses raw RLE-encoded bytes into slabs.
// Returns the slabs and total item count.
func rleLoad[T any](data []byte, slabSize int, packer Packer[T]) ([]Slab, int, error) {
	var state rleCursorState
	writer := NewSlabWriter[T](packer, slabSize, true)
	var lastOffset int

	for {
		run, err := rleTryNext(&state, data, packer)
		if err != nil {
			return nil, 0, err
		}
		if run == nil {
			break
		}

		if state.offset-lastOffset >= slabSize {
			if lastOffset < state.lastOffset {
				writer.Copy(data, lastOffset, state.lastOffset, 0, state.index-run.Count, state.acc.Sub(accForRun(run, packer)), nil)
			}
			writer.ManualSlabBreak()
			lastOffset = state.lastOffset
		}
	}

	// Copy remaining
	if lastOffset < state.offset {
		writer.Copy(data, lastOffset, state.offset, 0, state.index, state.acc, nil)
	}

	slabs := writer.Finish()
	return slabs, state.index, nil
}

// rleComputeMinMax computes min/max metadata for RLE-encoded slabs.
func rleComputeMinMax[T any](slabs []Slab, packer Packer[T]) {
	for i := range slabs {
		s := &slabs[i]
		var state rleCursorState
		data := s.Bytes()
		for {
			run, err := rleNext(&state, data, packer)
			if err != nil || run == nil {
				break
			}
		}
		s.SetMinMax(state.min, state.max)
	}
}

// accForRun computes the Acc for a single run.
func accForRun[T any](run *Run[T], packer Packer[T]) Acc {
	if run == nil || run.Value == nil {
		return Acc{}
	}
	return packer.ItemAgg(*run.Value).MulUint(run.Count)
}

// rleSplice performs a splice within a single RLE-encoded slab.
// This is a simplified implementation that decodes → modifies → re-encodes.
func rleSplice[T any](slab *Slab, index, del int, values []T, slabSize int, packer Packer[T], eq func(T, T) bool) SpliceResult {
	// Decode all values from the slab
	decoded := rleDecodeAll(slab, packer)

	// Compute overflow
	overflow := 0
	actualDel := del
	if index+del > len(decoded) {
		overflow = index + del - len(decoded)
		actualDel = len(decoded) - index
	}

	// Compute acc of deleted region
	var delAcc Acc
	for i := index; i < index+actualDel; i++ {
		if decoded[i] != nil {
			delAcc.AddAssign(packer.ItemAgg(*decoded[i]).MulUint(1))
		}
	}

	// Apply splice
	newLen := len(decoded) - actualDel + len(values)
	result := make([]*T, 0, newLen)
	result = append(result, decoded[:index]...)
	for i := range values {
		v := values[i]
		result = append(result, &v)
	}
	result = append(result, decoded[index+actualDel:]...)

	// Re-encode
	state := NewRleState[T](eq)
	writer := NewSlabWriter[T](packer, slabSize, false)
	for _, v := range result {
		state.Append(writer, v)
	}
	state.Flush(writer)
	slabs := writer.Finish()
	if len(slabs) == 0 {
		slabs = []Slab{{}}
	}

	// Compute min/max
	rleComputeMinMax(slabs, packer)

	return SpliceResult{
		Add:      len(values),
		Del:      actualDel,
		Overflow: overflow,
		Group:    delAcc,
		Slabs:    slabs,
	}
}

// rleDecodeAll decodes all values from a slab into a slice of *T (nil for nulls).
func rleDecodeAll[T any](slab *Slab, packer Packer[T]) []*T {
	var state rleCursorState
	data := slab.Bytes()
	result := make([]*T, 0, slab.Len())

	for {
		run, err := rleNext(&state, data, packer)
		if err != nil || run == nil {
			break
		}
		for i := 0; i < run.Count; i++ {
			if run.Value != nil {
				v := *run.Value
				result = append(result, &v)
			} else {
				result = append(result, nil)
			}
		}
	}
	return result
}

// rleEncode encodes values to bytes using RLE encoding.
func rleEncode[T any](values []T, packer Packer[T], eq func(T, T) bool) []byte {
	state := NewRleState[T](eq)
	writer := NewSlabWriter[T](packer, 1<<30, true) // locked, single slab
	for i := range values {
		state.Append(writer, &values[i])
	}
	state.Flush(writer)
	return writer.Write(nil)
}

// rleInitEmpty creates an empty slab representing length null items.
func rleInitEmpty[T any](length int, packer Packer[T]) Slab {
	if length == 0 {
		return Slab{}
	}
	writer := NewSlabWriter[T](packer, 1<<30, false)
	writer.FlushNull(length)
	slabs := writer.Finish()
	if len(slabs) == 0 {
		return Slab{}
	}
	return slabs[0]
}
