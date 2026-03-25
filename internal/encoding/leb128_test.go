package encoding

import (
	"math"
	"testing"
)

func TestAppendReadULEB128(t *testing.T) {
	tests := []struct {
		name string
		val  uint64
	}{
		{"zero", 0},
		{"one", 1},
		{"127", 127},
		{"128", 128},
		{"255", 255},
		{"256", 256},
		{"16383", 16383},
		{"16384", 16384},
		{"large", 1234567890},
		{"max_uint64", math.MaxUint64},
		{"max_uint32", math.MaxUint32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := AppendULEB128(nil, tt.val)
			got, offset, err := ReadULEB128(buf, 0)
			if err != nil {
				t.Fatalf("ReadULEB128 error: %v", err)
			}
			if offset != len(buf) {
				t.Fatalf("ReadULEB128 consumed %d bytes, expected %d", offset, len(buf))
			}
			if got != tt.val {
				t.Fatalf("ReadULEB128 = %d, want %d", got, tt.val)
			}
		})
	}
}

func TestAppendReadSLEB128(t *testing.T) {
	tests := []struct {
		name string
		val  int64
	}{
		{"zero", 0},
		{"one", 1},
		{"neg_one", -1},
		{"63", 63},
		{"64", 64},
		{"-64", -64},
		{"-65", -65},
		{"127", 127},
		{"128", 128},
		{"-128", -128},
		{"large_pos", 1234567890},
		{"large_neg", -1234567890},
		{"max_int64", math.MaxInt64},
		{"min_int64", math.MinInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := AppendSLEB128(nil, tt.val)
			got, offset, err := ReadSLEB128(buf, 0)
			if err != nil {
				t.Fatalf("ReadSLEB128 error: %v", err)
			}
			if offset != len(buf) {
				t.Fatalf("ReadSLEB128 consumed %d bytes, expected %d", offset, len(buf))
			}
			if got != tt.val {
				t.Fatalf("ReadSLEB128 = %d, want %d", got, tt.val)
			}
		})
	}
}

func TestULEBSize(t *testing.T) {
	tests := []struct {
		val      uint64
		expected int
	}{
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{math.MaxUint64, 10},
	}
	for _, tt := range tests {
		got := ULEBSize(tt.val)
		if got != tt.expected {
			t.Errorf("ULEBSize(%d) = %d, want %d", tt.val, got, tt.expected)
		}
		// Also verify it matches actual encoded length
		buf := AppendULEB128(nil, tt.val)
		if len(buf) != tt.expected {
			t.Errorf("AppendULEB128(%d) produced %d bytes, ULEBSize said %d", tt.val, len(buf), tt.expected)
		}
	}
}

func TestReadULEB128_Errors(t *testing.T) {
	// Empty data
	_, _, err := ReadULEB128(nil, 0)
	if err != ErrUnexpectedEOF {
		t.Errorf("expected ErrUnexpectedEOF, got %v", err)
	}

	// Truncated multi-byte
	_, _, err = ReadULEB128([]byte{0x80}, 0)
	if err != ErrUnexpectedEOF {
		t.Errorf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestReadSLEB128_Errors(t *testing.T) {
	_, _, err := ReadSLEB128(nil, 0)
	if err != ErrUnexpectedEOF {
		t.Errorf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestMultipleValuesInBuffer(t *testing.T) {
	buf := AppendULEB128(nil, 300)
	buf = AppendULEB128(buf, 42)
	buf = AppendULEB128(buf, 0)

	v1, off, err := ReadULEB128(buf, 0)
	if err != nil || v1 != 300 {
		t.Fatalf("first value: got %d, err %v", v1, err)
	}
	v2, off, err := ReadULEB128(buf, off)
	if err != nil || v2 != 42 {
		t.Fatalf("second value: got %d, err %v", v2, err)
	}
	v3, off, err := ReadULEB128(buf, off)
	if err != nil || v3 != 0 {
		t.Fatalf("third value: got %d, err %v", v3, err)
	}
	if off != len(buf) {
		t.Fatalf("expected to consume all bytes, got offset %d of %d", off, len(buf))
	}
}
