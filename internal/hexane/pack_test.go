package hexane

import "testing"

func TestUInt64PackRoundTrip(t *testing.T) {
	p := UInt64Packer{}
	values := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, 1<<32 - 1, 1 << 32, 1<<63 - 1}
	for _, v := range values {
		out := p.Pack(v, nil)
		got, n, err := p.Unpack(out)
		if err != nil {
			t.Fatalf("unpack %d: %v", v, err)
		}
		if n != len(out) {
			t.Errorf("unpack %d: consumed %d bytes, expected %d", v, n, len(out))
		}
		if got != v {
			t.Errorf("round-trip %d: got %d", v, got)
		}
		if w := p.Width(v); w != len(out) {
			t.Errorf("width %d: got %d, expected %d", v, w, len(out))
		}
	}
}

func TestInt64PackRoundTrip(t *testing.T) {
	p := Int64Packer{}
	values := []int64{0, 1, -1, 63, -64, 127, -128, 8191, -8192, 1<<31 - 1, -(1 << 31), 1<<62 - 1}
	for _, v := range values {
		out := p.Pack(v, nil)
		got, n, err := p.Unpack(out)
		if err != nil {
			t.Fatalf("unpack %d: %v", v, err)
		}
		if n != len(out) {
			t.Errorf("unpack %d: consumed %d bytes, expected %d", v, n, len(out))
		}
		if got != v {
			t.Errorf("round-trip %d: got %d", v, got)
		}
		if w := p.Width(v); w != len(out) {
			t.Errorf("width %d: got %d, expected %d", v, w, len(out))
		}
	}
}

func TestStrPackRoundTrip(t *testing.T) {
	p := StrPacker{}
	values := []string{"", "hello", "a longer string with spaces", "\x00\x01\x02"}
	for _, v := range values {
		out := p.Pack(v, nil)
		got, n, err := p.Unpack(out)
		if err != nil {
			t.Fatalf("unpack %q: %v", v, err)
		}
		if n != len(out) {
			t.Errorf("unpack %q: consumed %d bytes, expected %d", v, n, len(out))
		}
		if got != v {
			t.Errorf("round-trip %q: got %q", v, got)
		}
		if w := p.Width(v); w != len(out) {
			t.Errorf("width %q: got %d, expected %d", v, w, len(out))
		}
	}
}

func TestBytesPackRoundTrip(t *testing.T) {
	p := BytesPacker{}
	values := [][]byte{{}, {0, 1, 2}, {0xff, 0xfe, 0xfd}}
	for _, v := range values {
		out := p.Pack(v, nil)
		got, n, err := p.Unpack(out)
		if err != nil {
			t.Fatalf("unpack %x: %v", v, err)
		}
		if n != len(out) {
			t.Errorf("unpack %x: consumed %d bytes, expected %d", v, n, len(out))
		}
		if len(got) != len(v) {
			t.Errorf("round-trip %x: length %d, expected %d", v, len(got), len(v))
		}
		for i := range v {
			if got[i] != v[i] {
				t.Errorf("round-trip %x: byte %d differs", v, i)
			}
		}
	}
}

func TestUInt64Agg(t *testing.T) {
	p := UInt64Packer{}
	if a := p.ItemAgg(0); a.IsSome() {
		t.Error("Agg(0) should be None")
	}
	if a := p.ItemAgg(5); !a.IsSome() || a.AsUint64() != 5 {
		t.Error("Agg(5) should be Some(5)")
	}
}

func TestInt64Agg(t *testing.T) {
	p := Int64Packer{}
	if a := p.ItemAgg(0); a.IsSome() {
		t.Error("Agg(0) should be None")
	}
	if a := p.ItemAgg(5); !a.IsSome() || a.AsUint64() != 5 {
		t.Error("Agg(5) should be Some(5)")
	}
	if a := p.ItemAgg(-1); a.IsSome() {
		t.Error("Agg(-1) should be None")
	}
}

func TestBoolAgg(t *testing.T) {
	p := BoolPacker{}
	if a := p.ItemAgg(false); a.IsSome() {
		t.Error("Agg(false) should be None")
	}
	if a := p.ItemAgg(true); !a.IsSome() || a.AsUint64() != 1 {
		t.Error("Agg(true) should be Some(1)")
	}
}

func TestStrAgg(t *testing.T) {
	p := StrPacker{}
	if a := p.ItemAgg("hello"); a.IsSome() {
		t.Error("str Agg should always be None")
	}
}

func TestInt64Abs(t *testing.T) {
	p := Int64Packer{}
	if a := p.Abs(42); a != 42 {
		t.Errorf("Abs(42) = %d", a)
	}
	if a := p.Abs(-7); a != -7 {
		t.Errorf("Abs(-7) = %d", a)
	}
}
