package encoding

import "testing"

func TestDeltaRoundtrip(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
		nulls  []bool
	}{
		{"empty", nil, nil},
		{"single", []int64{42}, nil},
		{"ascending", []int64{1, 2, 3, 4, 5}, nil},
		{"descending", []int64{5, 4, 3, 2, 1}, nil},
		{"same", []int64{7, 7, 7, 7}, nil},
		{"mixed", []int64{0, 10, 10, 20, 15, 100}, nil},
		{"negative", []int64{-5, -3, -1, 0, 1, 3, 5}, nil},
		{"with_nulls", []int64{1, 0, 3, 0, 5}, []bool{false, true, false, true, false}},
		{"large_deltas", []int64{0, 1000000, 2000000, 3000000}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewDeltaEncoder()
			for i, v := range tt.values {
				if tt.nulls != nil && tt.nulls[i] {
					enc.AppendNull()
				} else {
					enc.AppendValue(v)
				}
			}
			buf := enc.Finish()

			dec := NewDeltaDecoder(buf)
			for i, expected := range tt.values {
				val, isNull, err := dec.Next()
				if err != nil {
					t.Fatalf("decode index %d: %v", i, err)
				}
				expectNull := tt.nulls != nil && tt.nulls[i]
				if isNull != expectNull {
					t.Fatalf("decode index %d: isNull=%v, want %v", i, isNull, expectNull)
				}
				if !expectNull && val != expected {
					t.Fatalf("decode index %d: got %d, want %d", i, val, expected)
				}
			}
			if !dec.Done() {
				t.Fatal("decoder not done")
			}
		})
	}
}
