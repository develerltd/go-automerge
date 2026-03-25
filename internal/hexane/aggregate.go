package hexane

// Agg is a per-item aggregate value, used for min/max slab metadata and accumulator steps.
//
// A zero Agg (valid=false) means the item contributes nothing to the accumulator
// (e.g. null values, strings, or byte columns). Non-zero values are used for range
// queries and accumulator-based navigation.
//
// Agg is produced by Packer.ItemAgg for each concrete item type.
type Agg struct {
	val   uint32
	valid bool
}

// AggFrom creates an Agg from a uint32. Zero maps to an invalid (None) Agg.
func AggFrom(v uint32) Agg {
	if v == 0 {
		return Agg{}
	}
	return Agg{val: v, valid: true}
}

// AggFromUint64 creates an Agg from a uint64, truncating to uint32.
// Zero maps to an invalid (None) Agg.
func AggFromUint64(v uint64) Agg {
	return AggFrom(uint32(v))
}

// AggFromInt64 creates an Agg from an int64. Negative or zero maps to invalid.
func AggFromInt64(v int64) Agg {
	if v <= 0 || v > int64(^uint32(0)) {
		return Agg{}
	}
	return Agg{val: uint32(v), valid: true}
}

// IsSome returns true if this Agg carries a value.
func (a Agg) IsSome() bool { return a.valid }

// IsNone returns true if this Agg carries no value.
func (a Agg) IsNone() bool { return !a.valid }

// AsUint64 returns the value as uint64, or 0 if None.
func (a Agg) AsUint64() uint64 {
	if !a.valid {
		return 0
	}
	return uint64(a.val)
}

// AsInt64 returns the value as int64 and whether it is valid.
func (a Agg) AsInt64() (int64, bool) {
	if !a.valid {
		return 0, false
	}
	return int64(a.val), true
}

// AsUint returns the value as int, or 0 if None.
func (a Agg) AsUint() int {
	if !a.valid {
		return 0
	}
	return int(a.val)
}

// Maximize returns the larger of a and other. None yields to any Some value.
func (a Agg) Maximize(other Agg) Agg {
	if !a.valid {
		return other
	}
	if other.valid && other.val > a.val {
		return other
	}
	return a
}

// Minimize returns the smaller of a and other. None yields to any Some value.
func (a Agg) Minimize(other Agg) Agg {
	if !a.valid {
		return other
	}
	if other.valid && other.val < a.val {
		return other
	}
	return a
}

// Less returns true if a < other. Comparison is only defined when both are valid.
func (a Agg) Less(other Agg) bool {
	return a.valid && other.valid && a.val < other.val
}

// Greater returns true if a > other. Comparison is only defined when both are valid.
func (a Agg) Greater(other Agg) bool {
	return a.valid && other.valid && a.val > other.val
}

// Equal returns true if both Agg values are identical.
func (a Agg) Equal(other Agg) bool {
	return a.valid == other.valid && a.val == other.val
}

// Sub subtracts other from a. Returns invalid if either is invalid.
func (a Agg) Sub(other Agg) Agg {
	if a.valid && other.valid {
		return AggFrom(a.val - other.val)
	}
	return Agg{}
}

// MulUint multiplies an Agg by n, producing an Acc.
func (a Agg) MulUint(n int) Acc {
	return Acc{val: a.AsUint64() * uint64(n)}
}

// Acc is a cumulative accumulator — the running sum of per-item Agg values.
//
// Its meaning depends on the cursor type:
//   - UIntCursor / IntCursor: sum of item values
//   - BooleanCursor: count of true values seen so far
//   - StrCursor / BytesCursor: always 0
type Acc struct {
	val uint64
}

// AccFrom creates an Acc from a uint64.
func AccFrom(v uint64) Acc {
	return Acc{val: v}
}

// AccFromInt creates an Acc from an int.
func AccFromInt(v int) Acc {
	return Acc{val: uint64(v)}
}

// Val returns the underlying uint64 value.
func (a Acc) Val() uint64 { return a.val }

// AsInt returns the value as int.
func (a Acc) AsInt() int { return int(a.val) }

// Add returns a + b.
func (a Acc) Add(b Acc) Acc { return Acc{val: a.val + b.val} }

// AddUint returns a + n.
func (a Acc) AddUint(n int) Acc { return Acc{val: a.val + uint64(n)} }

// AddAgg returns a + agg.
func (a Acc) AddAgg(agg Agg) Acc { return Acc{val: a.val + agg.AsUint64()} }

// Sub returns a - b.
func (a Acc) Sub(b Acc) Acc { return Acc{val: a.val - b.val} }

// SubSaturating returns a - b, clamped to 0.
func (a Acc) SubSaturating(b Acc) Acc {
	if a.val < b.val {
		return Acc{}
	}
	return Acc{val: a.val - b.val}
}

// SubUint returns a - n, with saturating subtraction.
func (a Acc) SubUint(n int) Acc {
	u := uint64(n)
	if a.val < u {
		return Acc{}
	}
	return Acc{val: a.val - u}
}

// SubAgg returns a - agg.AsUint64(), with saturating subtraction.
func (a Acc) SubAgg(agg Agg) Acc {
	u := agg.AsUint64()
	if a.val < u {
		return Acc{}
	}
	return Acc{val: a.val - u}
}

// AddAssign adds b in-place.
func (a *Acc) AddAssign(b Acc) { a.val += b.val }

// AddAssignAgg adds agg in-place.
func (a *Acc) AddAssignAgg(agg Agg) { a.val += agg.AsUint64() }

// SubAssign subtracts b in-place (saturating).
func (a *Acc) SubAssign(b Acc) {
	if a.val < b.val {
		a.val = 0
	} else {
		a.val -= b.val
	}
}

// SubAssignUint subtracts n in-place (saturating).
func (a *Acc) SubAssignUint(n int) {
	u := uint64(n)
	if a.val < u {
		a.val = 0
	} else {
		a.val -= u
	}
}

// MulUint returns a * n.
func (a Acc) MulUint(n int) Acc { return Acc{val: a.val * uint64(n)} }

// DivAgg returns a / agg as int.
func (a Acc) DivAgg(agg Agg) int {
	d := agg.AsUint64()
	if d == 0 {
		return 0
	}
	return int(a.val / d)
}

// Less returns true if a < b.
func (a Acc) Less(b Acc) bool { return a.val < b.val }

// LessEqual returns true if a <= b.
func (a Acc) LessEqual(b Acc) bool { return a.val <= b.val }

// Equal returns true if a == b.
func (a Acc) Equal(b Acc) bool { return a.val == b.val }

// IsZero returns true if the accumulator is zero.
func (a Acc) IsZero() bool { return a.val == 0 }
