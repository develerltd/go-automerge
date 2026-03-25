package encoding

import (
	"testing"
)

func encodeDecodeRLEUint64(t *testing.T, values []uint64, nulls []bool) {
	t.Helper()
	enc := NewRLEEncoderUint64()
	for i, v := range values {
		if nulls != nil && nulls[i] {
			enc.AppendNull()
		} else {
			enc.AppendValue(v)
		}
	}
	buf := enc.Finish()

	dec := NewRLEDecoderUint64(buf)
	for i, expected := range values {
		val, isNull, err := dec.Next()
		if err != nil {
			t.Fatalf("decode index %d: %v", i, err)
		}
		expectNull := nulls != nil && nulls[i]
		if isNull != expectNull {
			t.Fatalf("decode index %d: isNull=%v, want %v", i, isNull, expectNull)
		}
		if !expectNull && val != expected {
			t.Fatalf("decode index %d: got %d, want %d", i, val, expected)
		}
	}
	if !dec.Done() {
		t.Fatal("decoder not done after reading all values")
	}
}

func TestRLEEmpty(t *testing.T) {
	enc := NewRLEEncoderUint64()
	buf := enc.Finish()
	if len(buf) != 0 {
		t.Fatalf("expected empty output, got %d bytes", len(buf))
	}
}

func TestRLESingleValue(t *testing.T) {
	encodeDecodeRLEUint64(t, []uint64{42}, nil)
}

func TestRLERunOfSameValue(t *testing.T) {
	encodeDecodeRLEUint64(t, []uint64{5, 5, 5, 5, 5}, nil)
}

func TestRLELiteralRun(t *testing.T) {
	encodeDecodeRLEUint64(t, []uint64{1, 2, 3, 4, 5}, nil)
}

func TestRLEMixedRunsAndLiterals(t *testing.T) {
	encodeDecodeRLEUint64(t, []uint64{1, 2, 3, 5, 5, 5, 7, 8}, nil)
}

func TestRLENullRun(t *testing.T) {
	encodeDecodeRLEUint64(t,
		[]uint64{1, 0, 0, 0, 2},
		[]bool{false, true, true, true, false},
	)
}

func TestRLEAllNulls(t *testing.T) {
	// When all values are null (InitialNullRun), output should be empty
	enc := NewRLEEncoderUint64()
	enc.AppendNull()
	enc.AppendNull()
	enc.AppendNull()
	buf := enc.Finish()
	if len(buf) != 0 {
		t.Fatalf("all-null column should produce empty output, got %d bytes", len(buf))
	}
}

func TestRLENullThenValues(t *testing.T) {
	// Null run followed by values — the null run IS flushed since it's followed by real data
	enc := NewRLEEncoderUint64()
	enc.AppendNull()
	enc.AppendNull()
	enc.AppendValue(42)
	buf := enc.Finish()

	dec := NewRLEDecoderUint64(buf)
	// First: null run
	_, isNull, err := dec.Next()
	if err != nil || !isNull {
		t.Fatalf("expected null, got isNull=%v err=%v", isNull, err)
	}
	_, isNull, err = dec.Next()
	if err != nil || !isNull {
		t.Fatalf("expected null, got isNull=%v err=%v", isNull, err)
	}
	// Then: value
	val, isNull, err := dec.Next()
	if err != nil || isNull || val != 42 {
		t.Fatalf("expected 42, got val=%d isNull=%v err=%v", val, isNull, err)
	}
	if !dec.Done() {
		t.Fatal("decoder not done")
	}
}

func TestRLEAlternatingNullsAndValues(t *testing.T) {
	encodeDecodeRLEUint64(t,
		[]uint64{1, 0, 2, 0, 3},
		[]bool{false, true, false, true, false},
	)
}

func TestRLEString(t *testing.T) {
	enc := NewRLEEncoderString()
	values := []string{"hello", "hello", "world", "foo", "bar", "bar"}
	for _, v := range values {
		enc.AppendValue(v)
	}
	buf := enc.Finish()

	dec := NewRLEDecoderString(buf)
	for i, expected := range values {
		val, isNull, err := dec.Next()
		if err != nil {
			t.Fatalf("decode index %d: %v", i, err)
		}
		if isNull {
			t.Fatalf("decode index %d: unexpected null", i)
		}
		if val != expected {
			t.Fatalf("decode index %d: got %q, want %q", i, val, expected)
		}
	}
	if !dec.Done() {
		t.Fatal("decoder not done")
	}
}

func TestRLEInt64(t *testing.T) {
	enc := NewRLEEncoderInt64()
	values := []int64{-5, -5, -5, 0, 1, 2, 3, 3}
	for _, v := range values {
		enc.AppendValue(v)
	}
	buf := enc.Finish()

	dec := NewRLEDecoderInt64(buf)
	for i, expected := range values {
		val, isNull, err := dec.Next()
		if err != nil {
			t.Fatalf("decode index %d: %v", i, err)
		}
		if isNull {
			t.Fatalf("decode index %d: unexpected null", i)
		}
		if val != expected {
			t.Fatalf("decode index %d: got %d, want %d", i, val, expected)
		}
	}
}

// Test the state machine transitions: literal run followed by a matching value
// should flush the literal and start a new run of 2.
func TestRLELiteralToRun(t *testing.T) {
	encodeDecodeRLEUint64(t, []uint64{1, 2, 3, 3, 3}, nil)
}

// Test transition from run to literal
func TestRLERunToLiteral(t *testing.T) {
	encodeDecodeRLEUint64(t, []uint64{5, 5, 5, 1, 2, 3}, nil)
}

// Test lone value to null
func TestRLELoneValToNull(t *testing.T) {
	encodeDecodeRLEUint64(t,
		[]uint64{42, 0, 0},
		[]bool{false, true, true},
	)
}

// Test run to null
func TestRLERunToNull(t *testing.T) {
	encodeDecodeRLEUint64(t,
		[]uint64{5, 5, 5, 0, 0},
		[]bool{false, false, false, true, true},
	)
}

// Test literal run to null
func TestRLELiteralRunToNull(t *testing.T) {
	encodeDecodeRLEUint64(t,
		[]uint64{1, 2, 3, 0, 0},
		[]bool{false, false, false, true, true},
	)
}
