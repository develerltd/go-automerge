package encoding

// RLEEncoder implements run-length encoding matching the Automerge binary format exactly.
//
// Encoding format:
//   - Positive length (sLEB128) followed by value: run of identical values
//   - Zero (sLEB128) followed by count (uLEB128): run of null values
//   - Negative length (sLEB128) followed by |length| values: literal run of distinct values
//
// The state machine has states: Empty, InitialNullRun, NullRun, LoneVal, Run, LiteralRun.
// InitialNullRun is special: if the encoder finishes in this state, nothing is output
// (trailing/leading nulls are suppressed when they're the only thing).
type RLEEncoder[T comparable] struct {
	buf     []byte
	state   rleState[T]
	encode  func([]byte, T) []byte // encodes a single value of type T
}

type rleStateKind int

const (
	rleEmpty rleStateKind = iota
	rleInitialNullRun
	rleNullRun
	rleLoneVal
	rleRun
	rleLiteralRun
)

type rleState[T comparable] struct {
	kind  rleStateKind
	value T     // current value (for LoneVal, Run, LiteralRun)
	count int   // run length (for NullRun, InitialNullRun, Run)
	run   []T   // accumulated literal values (for LiteralRun)
}

// NewRLEEncoder creates an RLE encoder. The encode function encodes a single value T
// appending to the byte slice and returning the extended slice.
func NewRLEEncoder[T comparable](encode func([]byte, T) []byte) *RLEEncoder[T] {
	return &RLEEncoder[T]{
		state:  rleState[T]{kind: rleEmpty},
		encode: encode,
	}
}

// AppendValue appends a non-null value.
func (e *RLEEncoder[T]) AppendValue(value T) {
	switch e.state.kind {
	case rleEmpty:
		e.state = rleState[T]{kind: rleLoneVal, value: value}

	case rleLoneVal:
		if e.state.value == value {
			e.state = rleState[T]{kind: rleRun, value: value, count: 2}
		} else {
			run := make([]T, 1, 2)
			run[0] = e.state.value
			e.state = rleState[T]{kind: rleLiteralRun, value: value, run: run}
		}

	case rleRun:
		if e.state.value == value {
			e.state.count++
		} else {
			e.flushRun(e.state.value, e.state.count)
			e.state = rleState[T]{kind: rleLoneVal, value: value}
		}

	case rleLiteralRun:
		if e.state.value == value {
			e.flushLitRun(e.state.run)
			e.state = rleState[T]{kind: rleRun, value: value, count: 2}
		} else {
			e.state.run = append(e.state.run, e.state.value)
			e.state.value = value
		}

	case rleNullRun, rleInitialNullRun:
		e.flushNullRun(e.state.count)
		e.state = rleState[T]{kind: rleLoneVal, value: value}
	}
}

// AppendNull appends a null value.
func (e *RLEEncoder[T]) AppendNull() {
	switch e.state.kind {
	case rleEmpty:
		e.state = rleState[T]{kind: rleInitialNullRun, count: 1}

	case rleInitialNullRun:
		e.state.count++

	case rleNullRun:
		e.state.count++

	case rleLoneVal:
		e.flushLitRun([]T{e.state.value})
		e.state = rleState[T]{kind: rleNullRun, count: 1}

	case rleRun:
		e.flushRun(e.state.value, e.state.count)
		e.state = rleState[T]{kind: rleNullRun, count: 1}

	case rleLiteralRun:
		e.state.run = append(e.state.run, e.state.value)
		e.flushLitRun(e.state.run)
		e.state = rleState[T]{kind: rleNullRun, count: 1}
	}
}

// Append appends a value that may be null. If isNull is true, the value is ignored.
func (e *RLEEncoder[T]) Append(value T, isNull bool) {
	if isNull {
		e.AppendNull()
	} else {
		e.AppendValue(value)
	}
}

// Finish flushes the encoder and returns the encoded bytes.
func (e *RLEEncoder[T]) Finish() []byte {
	switch e.state.kind {
	case rleInitialNullRun:
		// Suppress: if entire column is null, output nothing
	case rleNullRun:
		e.flushNullRun(e.state.count)
	case rleLoneVal:
		e.flushLitRun([]T{e.state.value})
	case rleRun:
		e.flushRun(e.state.value, e.state.count)
	case rleLiteralRun:
		run := append(e.state.run, e.state.value)
		e.flushLitRun(run)
	case rleEmpty:
		// nothing
	}
	e.state = rleState[T]{kind: rleEmpty}
	return e.buf
}

func (e *RLEEncoder[T]) flushRun(val T, length int) {
	e.buf = AppendSLEB128(e.buf, int64(length))
	e.buf = e.encode(e.buf, val)
}

func (e *RLEEncoder[T]) flushNullRun(length int) {
	e.buf = AppendSLEB128(e.buf, 0)
	e.buf = AppendULEB128(e.buf, uint64(length))
}

func (e *RLEEncoder[T]) flushLitRun(run []T) {
	e.buf = AppendSLEB128(e.buf, -int64(len(run)))
	for _, val := range run {
		e.buf = e.encode(e.buf, val)
	}
}

// RLEDecoder decodes run-length encoded data.
type RLEDecoder[T any] struct {
	data      []byte
	offset    int
	lastValue *T
	count     int
	literal   bool
	decode    func([]byte, int) (T, int, error) // decodes a value from data at offset
}

// NewRLEDecoder creates an RLE decoder. The decode function reads a value of type T from
// data at the given offset and returns the value and new offset.
func NewRLEDecoder[T any](data []byte, decode func([]byte, int) (T, int, error)) *RLEDecoder[T] {
	return &RLEDecoder[T]{
		data:   data,
		decode: decode,
	}
}

// Done returns true if the decoder has consumed all data and has no remaining count.
func (d *RLEDecoder[T]) Done() bool {
	return d.offset >= len(d.data) && d.count == 0
}

// Next returns the next value. Returns (value, false, nil) for a non-null value,
// (zero, true, nil) for a null value, and (zero, false, err) for an error.
// When the decoder is exhausted, returns null (zero, true, nil).
func (d *RLEDecoder[T]) Next() (val T, isNull bool, err error) {
	var zero T
	for d.count == 0 {
		if d.offset >= len(d.data) {
			return zero, true, nil // exhausted = implicit null
		}
		count, newOff, err := ReadSLEB128(d.data, d.offset)
		if err != nil {
			return zero, false, err
		}
		d.offset = newOff

		if count > 0 {
			// Normal run: read the value
			d.count = int(count)
			v, newOff, err := d.decode(d.data, d.offset)
			if err != nil {
				return zero, false, err
			}
			d.offset = newOff
			d.lastValue = &v
			d.literal = false
		} else if count < 0 {
			// Literal run
			d.count = int(-count)
			d.literal = true
		} else {
			// Null run: read the count
			ucount, newOff, err := ReadULEB128(d.data, d.offset)
			if err != nil {
				return zero, false, err
			}
			d.offset = newOff
			d.count = int(ucount)
			d.lastValue = nil
			d.literal = false
		}
	}

	d.count--
	if d.literal {
		v, newOff, err := d.decode(d.data, d.offset)
		if err != nil {
			return zero, false, err
		}
		d.offset = newOff
		return v, false, nil
	}
	if d.lastValue == nil {
		return zero, true, nil
	}
	return *d.lastValue, false, nil
}

// Helpers for common RLE encoder/decoder types.

// NewRLEEncoderUint64 creates an RLE encoder for uint64 values (uLEB128 encoded).
func NewRLEEncoderUint64() *RLEEncoder[uint64] {
	return NewRLEEncoder(func(dst []byte, v uint64) []byte {
		return AppendULEB128(dst, v)
	})
}

// NewRLEEncoderInt64 creates an RLE encoder for int64 values (sLEB128 encoded).
func NewRLEEncoderInt64() *RLEEncoder[int64] {
	return NewRLEEncoder(func(dst []byte, v int64) []byte {
		return AppendSLEB128(dst, v)
	})
}

// NewRLEDecoderUint64 creates an RLE decoder for uint64 values.
func NewRLEDecoderUint64(data []byte) *RLEDecoder[uint64] {
	return NewRLEDecoder(data, ReadULEB128)
}

// NewRLEDecoderInt64 creates an RLE decoder for int64 values.
func NewRLEDecoderInt64(data []byte) *RLEDecoder[int64] {
	return NewRLEDecoder(data, ReadSLEB128)
}

// NewRLEEncoderString creates an RLE encoder for string values (length-prefixed UTF-8).
func NewRLEEncoderString() *RLEEncoder[string] {
	return NewRLEEncoder(func(dst []byte, v string) []byte {
		dst = AppendULEB128(dst, uint64(len(v)))
		return append(dst, v...)
	})
}

// NewRLEDecoderString creates an RLE decoder for string values (length-prefixed UTF-8).
func NewRLEDecoderString(data []byte) *RLEDecoder[string] {
	return NewRLEDecoder(data, ReadString)
}

// ReadString reads a length-prefixed UTF-8 string from data at offset.
func ReadString(data []byte, offset int) (string, int, error) {
	length, newOff, err := ReadULEB128(data, offset)
	if err != nil {
		return "", offset, err
	}
	end := newOff + int(length)
	if end > len(data) {
		return "", offset, ErrUnexpectedEOF
	}
	return string(data[newOff:end]), end, nil
}
