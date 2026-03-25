package hexane

import "testing"

func TestAggFromZero(t *testing.T) {
	a := AggFrom(0)
	if a.IsSome() {
		t.Error("AggFrom(0) should be None")
	}
	if a.AsUint64() != 0 {
		t.Error("None.AsUint64() should be 0")
	}
}

func TestAggFromNonZero(t *testing.T) {
	a := AggFrom(5)
	if !a.IsSome() {
		t.Error("AggFrom(5) should be Some")
	}
	if a.AsUint64() != 5 {
		t.Errorf("expected 5, got %d", a.AsUint64())
	}
}

func TestAggMaximize(t *testing.T) {
	none := Agg{}
	a := AggFrom(3)
	b := AggFrom(7)

	if r := none.Maximize(a); r != a {
		t.Error("None.Maximize(3) should be 3")
	}
	if r := a.Maximize(b); r != b {
		t.Error("3.Maximize(7) should be 7")
	}
	if r := b.Maximize(a); r != b {
		t.Error("7.Maximize(3) should be 7")
	}
}

func TestAggMinimize(t *testing.T) {
	none := Agg{}
	a := AggFrom(3)
	b := AggFrom(7)

	if r := none.Minimize(a); r != a {
		t.Error("None.Minimize(3) should be 3")
	}
	if r := a.Minimize(b); r != a {
		t.Error("3.Minimize(7) should be 3")
	}
	if r := b.Minimize(a); r != a {
		t.Error("7.Minimize(3) should be 3")
	}
}

func TestAggPartialOrder(t *testing.T) {
	a0 := AggFrom(0)
	a1 := AggFrom(1)
	a2 := AggFrom(2)

	if !a1.Less(a2) {
		t.Error("1 < 2")
	}
	if a2.Less(a1) {
		t.Error("2 < 1 should be false")
	}
	// None is not comparable
	if a0.Less(a1) {
		t.Error("None < 1 should be false (incomparable)")
	}
	if a1.Less(a0) {
		t.Error("1 < None should be false (incomparable)")
	}
}

func TestAccArithmetic(t *testing.T) {
	a := AccFrom(10)
	b := AccFrom(3)

	if r := a.Add(b); r.Val() != 13 {
		t.Errorf("10+3 = %d", r.Val())
	}
	if r := a.Sub(b); r.Val() != 7 {
		t.Errorf("10-3 = %d", r.Val())
	}
	if r := a.AddAgg(AggFrom(5)); r.Val() != 15 {
		t.Errorf("10+Agg(5) = %d", r.Val())
	}
	if r := a.SubSaturating(AccFrom(20)); r.Val() != 0 {
		t.Errorf("10-20 saturating = %d", r.Val())
	}
}

func TestAccAddAssign(t *testing.T) {
	a := AccFrom(5)
	a.AddAssign(AccFrom(3))
	if a.Val() != 8 {
		t.Errorf("5+=3 = %d", a.Val())
	}
	a.AddAssignAgg(AggFrom(2))
	if a.Val() != 10 {
		t.Errorf("8+=Agg(2) = %d", a.Val())
	}
}

func TestAccSubAssign(t *testing.T) {
	a := AccFrom(10)
	a.SubAssign(AccFrom(3))
	if a.Val() != 7 {
		t.Errorf("10-=3 = %d", a.Val())
	}
	a.SubAssign(AccFrom(100))
	if a.Val() != 0 {
		t.Errorf("7-=100 saturating = %d", a.Val())
	}
}

func TestAggMulUint(t *testing.T) {
	a := AggFrom(5)
	r := a.MulUint(3)
	if r.Val() != 15 {
		t.Errorf("Agg(5)*3 = %d", r.Val())
	}
}

func TestAccDivAgg(t *testing.T) {
	a := AccFrom(15)
	if r := a.DivAgg(AggFrom(5)); r != 3 {
		t.Errorf("15/Agg(5) = %d", r)
	}
	if r := a.DivAgg(Agg{}); r != 0 {
		t.Errorf("15/None = %d", r)
	}
}

func TestAggFromInt64(t *testing.T) {
	if a := AggFromInt64(-1); a.IsSome() {
		t.Error("negative should be None")
	}
	if a := AggFromInt64(0); a.IsSome() {
		t.Error("zero should be None")
	}
	if a := AggFromInt64(42); !a.IsSome() || a.AsUint64() != 42 {
		t.Error("42 should be Some(42)")
	}
}
