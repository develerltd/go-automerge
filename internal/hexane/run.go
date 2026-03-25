package hexane

// Run represents a single RLE run: Count consecutive repetitions of Value.
// Value is nil for null runs.
type Run[T any] struct {
	Count int
	Value *T
}

// IsNull returns true if this is a null run.
func (r Run[T]) IsNull() bool { return r.Value == nil }

// PopN returns a new Run with count reduced by n, or nil if count <= n.
func (r Run[T]) PopN(n int) *Run[T] {
	if r.Count <= n {
		return nil
	}
	return &Run[T]{Count: r.Count - n, Value: r.Value}
}

// Pop returns a new Run with count reduced by 1, or nil if count <= 1.
func (r Run[T]) Pop() *Run[T] {
	return r.PopN(1)
}

// Delta returns count * value for int64 runs (used by delta encoding).
func RunDelta(r *Run[int64]) int64 {
	if r.Value == nil {
		return 0
	}
	return int64(r.Count) * *r.Value
}

// SpliceResult is the outcome of a slab splice operation.
type SpliceResult struct {
	Add      int    // items added
	Del      int    // items deleted
	Overflow int    // deletion that overflows into next slab
	Group    Acc    // accumulator of spliced region
	Slabs    []Slab // resulting slabs
}
