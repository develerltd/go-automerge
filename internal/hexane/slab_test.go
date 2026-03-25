package hexane

import "testing"

func TestSlabBasic(t *testing.T) {
	s := NewSlab([]byte{1, 2, 3}, 10, AccFrom(50), 0)
	if s.Len() != 10 {
		t.Errorf("Len = %d", s.Len())
	}
	if s.ByteLen() != 3 {
		t.Errorf("ByteLen = %d", s.ByteLen())
	}
	if s.Acc().Val() != 50 {
		t.Errorf("Acc = %d", s.Acc().Val())
	}
	if s.Min().IsSome() {
		t.Error("Min should be None initially")
	}
	if s.Max().IsSome() {
		t.Error("Max should be None initially")
	}
}

func TestSlabMinMax(t *testing.T) {
	s := NewSlab(nil, 5, Acc{}, 0)
	s.SetMinMax(AggFrom(2), AggFrom(20))
	if s.Min() != AggFrom(2) {
		t.Error("Min != 2")
	}
	if s.Max() != AggFrom(20) {
		t.Error("Max != 20")
	}
}

func TestSlabWeighterAlloc(t *testing.T) {
	w := SlabWeighter()
	s := NewSlab([]byte{0}, 10, AccFrom(100), 0)
	s.SetMinMax(AggFrom(2), AggFrom(20))

	sw := w.Alloc(&s)
	if sw.Pos != 10 {
		t.Errorf("Pos = %d", sw.Pos)
	}
	if sw.Acc.Val() != 100 {
		t.Errorf("Acc = %d", sw.Acc.Val())
	}
	if sw.Min != AggFrom(2) {
		t.Error("Min mismatch")
	}
	if sw.Max != AggFrom(20) {
		t.Error("Max mismatch")
	}
}

func TestSlabWeighterAnd(t *testing.T) {
	w := SlabWeighter()
	a := SlabWeight{Pos: 10, Acc: AccFrom(100), Min: AggFrom(2), Max: AggFrom(20)}
	b := SlabWeight{Pos: 5, Acc: AccFrom(50), Min: AggFrom(3), Max: AggFrom(30)}

	c := w.And(a, b)
	if c.Pos != 15 {
		t.Errorf("Pos = %d", c.Pos)
	}
	if c.Acc.Val() != 150 {
		t.Errorf("Acc = %d", c.Acc.Val())
	}
	if c.Min != AggFrom(2) {
		t.Error("Min should be 2 (smaller)")
	}
	if c.Max != AggFrom(30) {
		t.Error("Max should be 30 (larger)")
	}
}

func TestSlabWeighterMaybeSub(t *testing.T) {
	w := SlabWeighter()

	baseline := SlabWeight{Pos: 100, Acc: AccFrom(100), Min: AggFrom(2), Max: AggFrom(20)}

	// Can subtract when removed max is strictly less than total max
	sub := SlabWeight{Pos: 50, Acc: AccFrom(50), Min: AggFrom(3), Max: AggFrom(19)}
	b := baseline
	if !w.MaybeSub(&b, sub) {
		t.Error("should succeed: removed max < total max and removed min > total min")
	}
	if b.Pos != 50 {
		t.Errorf("Pos after sub = %d", b.Pos)
	}

	// Cannot subtract when removed max equals total max (might invalidate)
	sub2 := SlabWeight{Pos: 50, Acc: AccFrom(50), Min: AggFrom(3), Max: AggFrom(20)}
	b2 := baseline
	if w.MaybeSub(&b2, sub2) {
		t.Error("should fail: removed max = total max")
	}

	// Can subtract when removed max is None
	sub3 := SlabWeight{Pos: 50, Acc: AccFrom(50), Min: AggFrom(3), Max: Agg{}}
	b3 := baseline
	if !w.MaybeSub(&b3, sub3) {
		t.Error("should succeed: removed max is None")
	}
}
