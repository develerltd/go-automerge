package opset

import (
	"testing"

	"github.com/develerltd/go-automerge/internal/types"
)

// --- Test helpers ---

func makeTestOps() []Op {
	return []Op{
		{
			ID:     types.OpId{Counter: 1, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "name"},
			Insert: false,
			Action: ActionSet,
			Value:  types.NewStr("Alice"),
		},
		{
			ID:     types.OpId{Counter: 2, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "age"},
			Insert: false,
			Action: ActionSet,
			Value:  types.NewUint(30),
		},
		{
			ID:     types.OpId{Counter: 3, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "list"},
			Insert: false,
			Action: ActionMakeList,
			Value:  types.NewNull(),
		},
		{
			ID:     types.OpId{Counter: 4, ActorIdx: 0},
			Obj:    types.ObjId{OpId: types.OpId{Counter: 3, ActorIdx: 0}},
			Key:    Key{Kind: KeySeq}, // HEAD
			Insert: true,
			Action: ActionSet,
			Value:  types.NewStr("hello"),
		},
		{
			ID:     types.OpId{Counter: 5, ActorIdx: 1},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "name"},
			Insert: false,
			Action: ActionSet,
			Value:  types.NewStr("Bob"),
			Succ:   []types.OpId{{Counter: 1, ActorIdx: 0}},
		},
	}
}

func assertOpEqual(t *testing.T, expected, actual *Op, label string) {
	t.Helper()
	if expected.ID != actual.ID {
		t.Errorf("%s: ID mismatch: expected %v, got %v", label, expected.ID, actual.ID)
	}
	if expected.Obj != actual.Obj {
		t.Errorf("%s: Obj mismatch: expected %v, got %v", label, expected.Obj, actual.Obj)
	}
	if expected.Key.Kind != actual.Key.Kind {
		t.Errorf("%s: Key.Kind mismatch: expected %v, got %v", label, expected.Key.Kind, actual.Key.Kind)
	}
	if expected.Key.Kind == KeyMap && expected.Key.MapKey != actual.Key.MapKey {
		t.Errorf("%s: Key.MapKey mismatch: expected %q, got %q", label, expected.Key.MapKey, actual.Key.MapKey)
	}
	if expected.Key.Kind == KeySeq && expected.Key.ElemID != actual.Key.ElemID {
		t.Errorf("%s: Key.ElemID mismatch: expected %v, got %v", label, expected.Key.ElemID, actual.Key.ElemID)
	}
	if expected.Insert != actual.Insert {
		t.Errorf("%s: Insert mismatch: expected %v, got %v", label, expected.Insert, actual.Insert)
	}
	if expected.Action != actual.Action {
		t.Errorf("%s: Action mismatch: expected %v, got %v", label, expected.Action, actual.Action)
	}
	if !expected.Value.Equal(actual.Value) {
		t.Errorf("%s: Value mismatch: expected %v, got %v", label, expected.Value, actual.Value)
	}
	if len(expected.Succ) != len(actual.Succ) {
		t.Errorf("%s: Succ length mismatch: expected %d, got %d", label, len(expected.Succ), len(actual.Succ))
	} else {
		for i := range expected.Succ {
			if expected.Succ[i] != actual.Succ[i] {
				t.Errorf("%s: Succ[%d] mismatch: expected %v, got %v", label, i, expected.Succ[i], actual.Succ[i])
			}
		}
	}
}

// --- NewOpColumns ---

func TestNewOpColumnsEmpty(t *testing.T) {
	c := NewOpColumns()
	if c.Len() != 0 {
		t.Errorf("expected Len=0, got %d", c.Len())
	}
	if !c.IsEmpty() {
		t.Error("expected IsEmpty=true")
	}
}

// --- Splice + Iterate round-trip ---

func TestOpColumnsSpliceAndIterate(t *testing.T) {
	ops := makeTestOps()
	c := NewOpColumns()
	c.Splice(0, ops)

	if c.Len() != len(ops) {
		t.Fatalf("expected Len=%d, got %d", len(ops), c.Len())
	}

	it := c.Iter()
	for i, expected := range ops {
		actual, ok := it.Next()
		if !ok {
			t.Fatalf("iterator exhausted at op %d", i)
		}
		assertOpEqual(t, &expected, actual, ops[i].ID.String())
	}

	// Should be exhausted
	_, ok := it.Next()
	if ok {
		t.Error("expected iterator to be exhausted")
	}
}

// --- ToOps ---

func TestOpColumnsToOps(t *testing.T) {
	ops := makeTestOps()
	c := NewOpColumns()
	c.Splice(0, ops)

	result := c.ToOps()
	if len(result) != len(ops) {
		t.Fatalf("expected %d ops, got %d", len(ops), len(result))
	}
	for i := range ops {
		assertOpEqual(t, &ops[i], &result[i], ops[i].ID.String())
	}
}

// --- Export + Load round-trip ---

func TestOpColumnsExportLoadRoundTrip(t *testing.T) {
	ops := makeTestOps()
	c := NewOpColumns()
	c.Splice(0, ops)

	// Export to raw columns
	rawCols := c.Export()

	// Load back
	c2, err := LoadOpColumns(rawCols)
	if err != nil {
		t.Fatalf("LoadOpColumns: %v", err)
	}

	if c2.Len() != len(ops) {
		t.Fatalf("loaded Len=%d, expected %d", c2.Len(), len(ops))
	}

	result := c2.ToOps()
	for i := range ops {
		assertOpEqual(t, &ops[i], &result[i], ops[i].ID.String())
	}
}

// --- Cross-compatibility with existing encoder ---

func TestOpColumnsCompatWithEncodeDocOps(t *testing.T) {
	ops := makeTestOps()

	// Encode using the existing flat encoder
	rawCols := EncodeDocOps(ops)

	// Load into hexane OpColumns
	c, err := LoadOpColumns(rawCols)
	if err != nil {
		t.Fatalf("LoadOpColumns from EncodeDocOps: %v", err)
	}

	if c.Len() != len(ops) {
		t.Fatalf("loaded Len=%d, expected %d", c.Len(), len(ops))
	}

	result := c.ToOps()
	for i := range ops {
		assertOpEqual(t, &ops[i], &result[i], ops[i].ID.String())
	}
}

// --- IterRange ---

func TestOpColumnsIterRange(t *testing.T) {
	ops := makeTestOps()
	c := NewOpColumns()
	c.Splice(0, ops)

	// Iterate a sub-range
	it := c.IterRange(1, 3)
	for i := 1; i < 3; i++ {
		actual, ok := it.Next()
		if !ok {
			t.Fatalf("iterator exhausted at op %d", i)
		}
		assertOpEqual(t, &ops[i], actual, ops[i].ID.String())
	}
	_, ok := it.Next()
	if ok {
		t.Error("expected iterator to be exhausted after range")
	}
}

// --- Value types ---

func TestOpColumnsAllValueTypes(t *testing.T) {
	ops := []Op{
		{ID: types.OpId{Counter: 1, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "null"}, Action: ActionSet, Value: types.NewNull()},
		{ID: types.OpId{Counter: 2, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "false"}, Action: ActionSet, Value: types.NewBool(false)},
		{ID: types.OpId{Counter: 3, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "true"}, Action: ActionSet, Value: types.NewBool(true)},
		{ID: types.OpId{Counter: 4, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "uint"}, Action: ActionSet, Value: types.NewUint(42)},
		{ID: types.OpId{Counter: 5, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "int"}, Action: ActionSet, Value: types.NewInt(-99)},
		{ID: types.OpId{Counter: 6, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "f64"}, Action: ActionSet, Value: types.NewFloat64(3.14)},
		{ID: types.OpId{Counter: 7, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "str"}, Action: ActionSet, Value: types.NewStr("hello world")},
		{ID: types.OpId{Counter: 8, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "bytes"}, Action: ActionSet, Value: types.NewBytes([]byte{0xDE, 0xAD, 0xBE, 0xEF})},
		{ID: types.OpId{Counter: 9, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "counter"}, Action: ActionSet, Value: types.NewCounter(100)},
		{ID: types.OpId{Counter: 10, ActorIdx: 0}, Obj: types.Root, Key: Key{Kind: KeyMap, MapKey: "ts"}, Action: ActionSet, Value: types.NewTimestamp(1700000000)},
	}

	c := NewOpColumns()
	c.Splice(0, ops)

	// Export + load round trip
	rawCols := c.Export()
	c2, err := LoadOpColumns(rawCols)
	if err != nil {
		t.Fatalf("LoadOpColumns: %v", err)
	}

	result := c2.ToOps()
	if len(result) != len(ops) {
		t.Fatalf("expected %d ops, got %d", len(ops), len(result))
	}
	for i := range ops {
		assertOpEqual(t, &ops[i], &result[i], ops[i].ID.String())
	}
}

// --- Successors ---

func TestOpColumnsMultipleSuccessors(t *testing.T) {
	ops := []Op{
		{
			ID:     types.OpId{Counter: 1, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "x"},
			Action: ActionSet,
			Value:  types.NewStr("old"),
			Succ: []types.OpId{
				{Counter: 3, ActorIdx: 0},
				{Counter: 4, ActorIdx: 1},
			},
		},
		{
			ID:     types.OpId{Counter: 2, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "y"},
			Action: ActionSet,
			Value:  types.NewStr("keep"),
		},
		{
			ID:     types.OpId{Counter: 3, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "x"},
			Action: ActionSet,
			Value:  types.NewStr("new1"),
		},
		{
			ID:     types.OpId{Counter: 4, ActorIdx: 1},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "x"},
			Action: ActionSet,
			Value:  types.NewStr("new2"),
		},
	}

	c := NewOpColumns()
	c.Splice(0, ops)

	result := c.ToOps()
	// Op 0 should have 2 successors
	if len(result[0].Succ) != 2 {
		t.Fatalf("expected 2 successors, got %d", len(result[0].Succ))
	}
	if result[0].Succ[0] != (types.OpId{Counter: 3, ActorIdx: 0}) {
		t.Errorf("succ[0] mismatch: %v", result[0].Succ[0])
	}
	if result[0].Succ[1] != (types.OpId{Counter: 4, ActorIdx: 1}) {
		t.Errorf("succ[1] mismatch: %v", result[0].Succ[1])
	}
	// Op 1 should have 0 successors
	if len(result[1].Succ) != 0 {
		t.Errorf("expected 0 successors, got %d", len(result[1].Succ))
	}
}

// --- Sequence keys ---

func TestOpColumnsSequenceKeys(t *testing.T) {
	listObj := types.ObjId{OpId: types.OpId{Counter: 1, ActorIdx: 0}}
	ops := []Op{
		{
			ID:     types.OpId{Counter: 1, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "list"},
			Action: ActionMakeList,
			Value:  types.NewNull(),
		},
		{
			ID:     types.OpId{Counter: 2, ActorIdx: 0},
			Obj:    listObj,
			Key:    Key{Kind: KeySeq}, // HEAD (zero ElemID)
			Insert: true,
			Action: ActionSet,
			Value:  types.NewStr("first"),
		},
		{
			ID:  types.OpId{Counter: 3, ActorIdx: 0},
			Obj: listObj,
			Key: Key{Kind: KeySeq, ElemID: types.OpId{Counter: 2, ActorIdx: 0}},
			// Target previous element
			Insert: true,
			Action: ActionSet,
			Value:  types.NewStr("second"),
		},
	}

	c := NewOpColumns()
	c.Splice(0, ops)

	result := c.ToOps()
	// Op 1: HEAD target
	if result[1].Key.Kind != KeySeq {
		t.Errorf("expected KeySeq, got %d", result[1].Key.Kind)
	}
	if !result[1].Key.ElemID.IsZero() {
		t.Errorf("expected HEAD (zero ElemID), got %v", result[1].Key.ElemID)
	}
	// Op 2: targets op 2
	if result[2].Key.ElemID != (types.OpId{Counter: 2, ActorIdx: 0}) {
		t.Errorf("expected ElemID 2@0, got %v", result[2].Key.ElemID)
	}
}

// --- Actor remapping ---

func TestOpColumnsRewriteWithNewActor(t *testing.T) {
	ops := []Op{
		{
			ID:     types.OpId{Counter: 1, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "a"},
			Action: ActionSet,
			Value:  types.NewStr("x"),
		},
		{
			ID:     types.OpId{Counter: 2, ActorIdx: 1},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "b"},
			Action: ActionSet,
			Value:  types.NewStr("y"),
			Succ:   []types.OpId{{Counter: 3, ActorIdx: 2}},
		},
	}

	c := NewOpColumns()
	c.Splice(0, ops)

	// Insert new actor at index 1 — actors >= 1 should be bumped
	c.RewriteWithNewActor(1)

	result := c.ToOps()
	// Actor 0 should stay 0
	if result[0].ID.ActorIdx != 0 {
		t.Errorf("expected actor 0, got %d", result[0].ID.ActorIdx)
	}
	// Actor 1 should become 2
	if result[1].ID.ActorIdx != 2 {
		t.Errorf("expected actor 2, got %d", result[1].ID.ActorIdx)
	}
	// Succ actor 2 should become 3
	if result[1].Succ[0].ActorIdx != 3 {
		t.Errorf("expected succ actor 3, got %d", result[1].Succ[0].ActorIdx)
	}
}

func TestOpColumnsRewriteWithoutActor(t *testing.T) {
	ops := []Op{
		{
			ID:     types.OpId{Counter: 1, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "a"},
			Action: ActionSet,
			Value:  types.NewStr("x"),
		},
		{
			ID:     types.OpId{Counter: 2, ActorIdx: 2},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "b"},
			Action: ActionSet,
			Value:  types.NewStr("y"),
		},
	}

	c := NewOpColumns()
	c.Splice(0, ops)

	// Remove actor at index 1 — actors > 1 should be decremented
	c.RewriteWithoutActor(1)

	result := c.ToOps()
	if result[0].ID.ActorIdx != 0 {
		t.Errorf("expected actor 0, got %d", result[0].ID.ActorIdx)
	}
	if result[1].ID.ActorIdx != 1 {
		t.Errorf("expected actor 1, got %d", result[1].ID.ActorIdx)
	}
}

// --- Load empty columns ---

func TestLoadOpColumnsEmpty(t *testing.T) {
	c, err := LoadOpColumns(nil)
	if err != nil {
		t.Fatalf("LoadOpColumns(nil): %v", err)
	}
	if c.Len() != 0 {
		t.Errorf("expected Len=0, got %d", c.Len())
	}
}

// --- Mark operations ---

func TestOpColumnsMarks(t *testing.T) {
	textObj := types.ObjId{OpId: types.OpId{Counter: 1, ActorIdx: 0}}
	ops := []Op{
		{
			ID:     types.OpId{Counter: 1, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "text"},
			Action: ActionMakeText,
			Value:  types.NewNull(),
		},
		{
			ID:       types.OpId{Counter: 2, ActorIdx: 0},
			Obj:      textObj,
			Key:      Key{Kind: KeySeq},
			Insert:   true,
			Action:   ActionSet,
			Value:    types.NewStr("a"),
			MarkName: "",
		},
		{
			ID:       types.OpId{Counter: 3, ActorIdx: 0},
			Obj:      textObj,
			Key:      Key{Kind: KeySeq, ElemID: types.OpId{Counter: 2, ActorIdx: 0}},
			Action:   ActionMark,
			Value:    types.NewBool(true),
			MarkName: "bold",
			Expand:   true,
		},
	}

	c := NewOpColumns()
	c.Splice(0, ops)

	result := c.ToOps()
	if result[2].MarkName != "bold" {
		t.Errorf("expected mark name 'bold', got %q", result[2].MarkName)
	}
	if !result[2].Expand {
		t.Error("expected expand=true")
	}
}

// --- Splice at middle ---

func TestOpColumnsSpliceAtMiddle(t *testing.T) {
	ops := makeTestOps()
	c := NewOpColumns()
	c.Splice(0, ops)

	// Insert a new op at position 2
	newOp := Op{
		ID:     types.OpId{Counter: 100, ActorIdx: 0},
		Obj:    types.Root,
		Key:    Key{Kind: KeyMap, MapKey: "inserted"},
		Action: ActionSet,
		Value:  types.NewStr("middle"),
	}
	c.Splice(2, []Op{newOp})

	if c.Len() != len(ops)+1 {
		t.Fatalf("expected Len=%d, got %d", len(ops)+1, c.Len())
	}

	result := c.ToOps()
	// First two ops unchanged
	assertOpEqual(t, &ops[0], &result[0], "op[0]")
	assertOpEqual(t, &ops[1], &result[1], "op[1]")
	// Inserted op at position 2
	assertOpEqual(t, &newOp, &result[2], "inserted")
	// Remaining ops shifted
	assertOpEqual(t, &ops[2], &result[3], "op[2]→3")
	assertOpEqual(t, &ops[3], &result[4], "op[3]→4")
	assertOpEqual(t, &ops[4], &result[5], "op[4]→5")
}

// --- Large value ---

func TestOpColumnsLargeStringValue(t *testing.T) {
	// Create a string larger than typical slab sizes
	largeStr := make([]byte, 500)
	for i := range largeStr {
		largeStr[i] = byte('A' + i%26)
	}

	ops := []Op{
		{
			ID:     types.OpId{Counter: 1, ActorIdx: 0},
			Obj:    types.Root,
			Key:    Key{Kind: KeyMap, MapKey: "big"},
			Action: ActionSet,
			Value:  types.NewStr(string(largeStr)),
		},
	}

	c := NewOpColumns()
	c.Splice(0, ops)

	// Round-trip through export/load
	rawCols := c.Export()
	c2, err := LoadOpColumns(rawCols)
	if err != nil {
		t.Fatalf("LoadOpColumns: %v", err)
	}

	result := c2.ToOps()
	if result[0].Value.Str() != string(largeStr) {
		t.Errorf("large string value mismatch: got length %d", len(result[0].Value.Str()))
	}
}
