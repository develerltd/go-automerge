package hexane

// Weighter provides the SpanWeight operations for a SpanTree[T, W].
//
// In Rust, SpanWeight is a trait with associated methods. Go interfaces can't
// be self-referential in the required way, so we use a function table instead.
// This is set once at SpanTree construction and used for all weight operations.
type Weighter[T, W any] struct {
	// Zero is the default/identity weight value.
	Zero W

	// Alloc creates a weight from a single element.
	Alloc func(span *T) W

	// And combines two weights (commutative binary composition).
	And func(a, b W) W

	// Union accumulates other into a (in-place: a = a AND other).
	Union func(a *W, b W)

	// MaybeSub tries to subtract b from a. Returns true if successful,
	// false if a full recomputation is needed (e.g., when min/max would be
	// invalidated by the removal).
	MaybeSub func(a *W, b W) bool
}

// UnitWeighter returns a Weighter for unit type (no weight tracking).
// Useful for SpanTrees that don't need aggregation.
func UnitWeighter[T any]() Weighter[T, struct{}] {
	return Weighter[T, struct{}]{
		Zero:     struct{}{},
		Alloc:    func(_ *T) struct{} { return struct{}{} },
		And:      func(_, _ struct{}) struct{} { return struct{}{} },
		Union:    func(_ *struct{}, _ struct{}) {},
		MaybeSub: func(_ *struct{}, _ struct{}) bool { return true },
	}
}
