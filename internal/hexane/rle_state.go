package hexane

type rleStateKind int

const (
	rleEmpty    rleStateKind = iota
	rleLoneValue             // single buffered value
	rleRun                   // repeated value
	rleLitRun                // literal run of distinct values
)

// RleState is the encoder state machine for RLE cursors.
// It buffers incoming values and emits encoded runs to a SlabWriter.
//
// The state machine merges adjacent equal values into runs, accumulates
// different single values into literal runs, and flushes when the pattern
// changes.
type RleState[T any] struct {
	kind    rleStateKind
	value   *T       // lone value or run value (nil = null)
	count   int      // run count
	litRun  []T      // accumulated lit run values (not including current)
	current *T       // current value in lit run
	equal   func(T, T) bool
}

// NewRleState creates a new RLE encoder state with the given equality function.
func NewRleState[T any](eq func(T, T) bool) *RleState[T] {
	return &RleState[T]{kind: rleEmpty, equal: eq}
}

func (s *RleState[T]) valuesEqual(a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return s.equal(*a, *b)
}

// IsEmpty implements EncoderState.
func (s *RleState[T]) IsEmpty() bool {
	switch s.kind {
	case rleEmpty:
		return true
	case rleLoneValue:
		return s.value == nil
	case rleRun:
		return s.value == nil
	}
	return false
}

// Append implements EncoderState.
func (s *RleState[T]) Append(sw *SlabWriter[T], value *T) int {
	return s.AppendChunk(sw, Run[T]{Count: 1, Value: value})
}

// AppendChunk implements EncoderState.
func (s *RleState[T]) AppendChunk(sw *SlabWriter[T], run Run[T]) int {
	count := run.Count

	switch s.kind {
	case rleEmpty:
		s.fromRun(run)

	case rleLoneValue:
		if s.valuesEqual(s.value, run.Value) {
			s.kind = rleRun
			s.count = count + 1
			s.value = run.Value
		} else if run.Value != nil && s.value != nil && count == 1 {
			s.kind = rleLitRun
			s.litRun = s.litRun[:0]
			s.litRun = append(s.litRun, *s.value)
			v := *run.Value
			s.current = &v
		} else {
			s.flushSingle(sw)
			s.fromRun(run)
		}

	case rleRun:
		if s.valuesEqual(s.value, run.Value) {
			s.count += count
		} else {
			s.flushRunValues(sw)
			s.fromRun(run)
		}

	case rleLitRun:
		if s.current != nil && s.valuesEqual(s.current, run.Value) {
			// End of lit run merges with next
			sw.FlushLitRun(s.litRun)
			s.litRun = s.litRun[:0]
			s.kind = rleRun
			s.count = count + 1
			s.value = run.Value
			s.current = nil
		} else if count == 1 && run.Value != nil {
			// Single different value — add to lit run
			s.litRun = append(s.litRun, *s.current)
			v := *run.Value
			s.current = &v
		} else {
			// Flush lit run and start new
			if s.current != nil {
				s.litRun = append(s.litRun, *s.current)
			}
			sw.FlushLitRun(s.litRun)
			s.litRun = s.litRun[:0]
			s.current = nil
			s.fromRun(run)
		}
	}

	return count
}

// Flush implements EncoderState.
func (s *RleState[T]) Flush(sw *SlabWriter[T]) {
	switch s.kind {
	case rleEmpty:
		// nothing
	case rleLoneValue:
		s.flushSingle(sw)
	case rleRun:
		s.flushRunValues(sw)
	case rleLitRun:
		if s.current != nil {
			s.litRun = append(s.litRun, *s.current)
		}
		sw.FlushLitRun(s.litRun)
		s.litRun = s.litRun[:0]
	}
	s.kind = rleEmpty
	s.value = nil
	s.count = 0
	s.current = nil
}

func (s *RleState[T]) fromRun(run Run[T]) {
	if run.Count == 1 {
		s.kind = rleLoneValue
		s.value = run.Value
	} else {
		s.kind = rleRun
		s.count = run.Count
		s.value = run.Value
	}
}

func (s *RleState[T]) flushSingle(sw *SlabWriter[T]) {
	if s.value != nil {
		sw.FlushLitRun([]T{*s.value})
	} else {
		sw.FlushNull(1)
	}
}

func (s *RleState[T]) flushRunValues(sw *SlabWriter[T]) {
	if s.value != nil {
		if s.count == 1 {
			sw.FlushLitRun([]T{*s.value})
		} else {
			sw.FlushRun(s.count, *s.value)
		}
	} else {
		sw.FlushNull(s.count)
	}
}
