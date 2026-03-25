package encoding

import "testing"

func TestBooleanRoundtrip(t *testing.T) {
	tests := []struct {
		name   string
		values []bool
	}{
		{"empty", nil},
		{"single_false", []bool{false}},
		{"single_true", []bool{true}},
		{"all_false", []bool{false, false, false}},
		{"all_true", []bool{true, true, true}},
		{"alternating", []bool{false, true, false, true}},
		{"starts_true", []bool{true, true, false, false, true}},
		{"mixed", []bool{false, false, true, false, true, true, true, false}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.values) == 0 {
				return
			}
			enc := NewBooleanEncoder()
			for _, v := range tt.values {
				enc.Append(v)
			}
			buf := enc.Finish()

			dec := NewBooleanDecoder(buf)
			for i, expected := range tt.values {
				val, err := dec.Next()
				if err != nil {
					t.Fatalf("decode index %d: %v", i, err)
				}
				if val != expected {
					t.Fatalf("decode index %d: got %v, want %v", i, val, expected)
				}
			}
			if !dec.Done() {
				t.Fatal("decoder not done")
			}
		})
	}
}

func TestBooleanStartsWithFalse(t *testing.T) {
	// The encoding always starts with a count of false values.
	// If the first value is true, the first count should be 0.
	enc := NewBooleanEncoder()
	enc.Append(true)
	enc.Append(true)
	enc.Append(false)
	buf := enc.Finish()

	// First byte should be 0 (zero falses), then 2 (two trues), then 1 (one false)
	dec := NewBooleanDecoder(buf)
	v, err := dec.Next()
	if err != nil || v != true {
		t.Fatalf("expected true, got %v err=%v", v, err)
	}
	v, err = dec.Next()
	if err != nil || v != true {
		t.Fatalf("expected true, got %v err=%v", v, err)
	}
	v, err = dec.Next()
	if err != nil || v != false {
		t.Fatalf("expected false, got %v err=%v", v, err)
	}
}

func TestMaybeBooleanAllFalse(t *testing.T) {
	enc := NewMaybeBooleanEncoder()
	enc.Append(false)
	enc.Append(false)
	enc.Append(false)
	buf := enc.Finish()
	if buf != nil {
		t.Fatalf("expected nil output for all-false, got %d bytes", len(buf))
	}
}

func TestMaybeBooleanWithTrue(t *testing.T) {
	enc := NewMaybeBooleanEncoder()
	enc.Append(false)
	enc.Append(true)
	enc.Append(false)
	buf := enc.Finish()
	if buf == nil {
		t.Fatal("expected non-nil output when a true value exists")
	}

	dec := NewBooleanDecoder(buf)
	vals := []bool{false, true, false}
	for i, expected := range vals {
		v, err := dec.Next()
		if err != nil {
			t.Fatalf("decode index %d: %v", i, err)
		}
		if v != expected {
			t.Fatalf("decode index %d: got %v, want %v", i, v, expected)
		}
	}
}
