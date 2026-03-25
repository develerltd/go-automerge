package hexane

// Encoder is a streaming encoder that accumulates values and writes them into slabs.
//
// It wraps an EncoderState (RLE, Boolean, Delta, or Raw state machine) and a SlabWriter.
// Values are buffered by the state machine and periodically flushed as encoded runs
// to the SlabWriter.
type Encoder[T any] struct {
	Len    int
	State  EncoderState[T]
	Writer *SlabWriter[T]
}

// NewEncoder creates a streaming encoder with the given state and packer.
// If singleSlab is true, the encoder produces a single slab (no splitting).
func NewEncoder[T any](state EncoderState[T], packer Packer[T], slabSize int, singleSlab bool) *Encoder[T] {
	return &Encoder[T]{
		State:  state,
		Writer: NewSlabWriter[T](packer, slabSize, singleSlab),
	}
}

// Append adds a single value (nil for null) to the encoder.
func (e *Encoder[T]) Append(value *T) int {
	items := e.State.Append(e.Writer, value)
	e.Len += items
	return items
}

// AppendValue adds a non-null value to the encoder.
func (e *Encoder[T]) AppendValue(value T) int {
	return e.Append(&value)
}

// AppendNull adds a null value to the encoder.
func (e *Encoder[T]) AppendNull() int {
	return e.Append(nil)
}

// AppendChunk adds a run of values to the encoder.
func (e *Encoder[T]) AppendChunk(run Run[T]) int {
	e.Len += run.Count
	return e.State.AppendChunk(e.Writer, run)
}

// Flush flushes any buffered state to the writer.
func (e *Encoder[T]) Flush() {
	e.State.Flush(e.Writer)
}

// SaveTo flushes and appends all encoded bytes to out, returning the extended slice.
// Only valid for encoders created with singleSlab=true.
func (e *Encoder[T]) SaveTo(out []byte) []byte {
	e.State.Flush(e.Writer)
	if e.Len > 0 {
		return e.Writer.Write(out)
	}
	return out
}

// IntoSlabs flushes and returns the produced slabs.
func (e *Encoder[T]) IntoSlabs(computeMinMax func([]Slab)) []Slab {
	e.State.Flush(e.Writer)
	slabs := e.Writer.Finish()
	if computeMinMax != nil {
		computeMinMax(slabs)
	}
	return slabs
}

// IsEmpty returns true if no data has been written.
func (e *Encoder[T]) IsEmpty() bool {
	return e.Writer.IsEmpty() && e.State.IsEmpty()
}
