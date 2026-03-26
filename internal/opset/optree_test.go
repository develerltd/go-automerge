package opset

import (
	"testing"

	"github.com/develerltd/go-automerge/internal/types"
)

func TestOpTreeInsertAndAllOps(t *testing.T) {
	tree := NewOpTree()

	ops := []Op{
		{ID: types.OpId{Counter: 3, ActorIdx: 0}, Key: Key{Kind: KeyMap, MapKey: "b"}, Action: ActionSet, Value: types.NewStr("val3")},
		{ID: types.OpId{Counter: 1, ActorIdx: 0}, Key: Key{Kind: KeyMap, MapKey: "a"}, Action: ActionSet, Value: types.NewStr("val1")},
		{ID: types.OpId{Counter: 2, ActorIdx: 0}, Key: Key{Kind: KeyMap, MapKey: "a"}, Action: ActionSet, Value: types.NewStr("val2")},
	}
	for _, op := range ops {
		tree.Insert(op)
	}

	if tree.Len() != 3 {
		t.Fatalf("expected 3, got %d", tree.Len())
	}

	all := tree.AllOps()
	// Should be sorted: a@1, a@2, b@3
	if all[0].ID.Counter != 1 || all[0].Key.MapKey != "a" {
		t.Errorf("first op: got %v", all[0])
	}
	if all[1].ID.Counter != 2 || all[1].Key.MapKey != "a" {
		t.Errorf("second op: got %v", all[1])
	}
	if all[2].ID.Counter != 3 || all[2].Key.MapKey != "b" {
		t.Errorf("third op: got %v", all[2])
	}
}

func TestOpTreeOpsForMapKey(t *testing.T) {
	tree := NewOpTree()
	tree.Insert(Op{ID: types.OpId{Counter: 1}, Key: Key{Kind: KeyMap, MapKey: "a"}, Action: ActionSet})
	tree.Insert(Op{ID: types.OpId{Counter: 2}, Key: Key{Kind: KeyMap, MapKey: "b"}, Action: ActionSet})
	tree.Insert(Op{ID: types.OpId{Counter: 3}, Key: Key{Kind: KeyMap, MapKey: "a"}, Action: ActionSet})

	ops := tree.OpsForMapKey("a")
	if len(ops) != 2 {
		t.Fatalf("expected 2 ops for key 'a', got %d", len(ops))
	}
	if ops[0].ID.Counter != 1 || ops[1].ID.Counter != 3 {
		t.Errorf("wrong ops: %v", ops)
	}

	ops = tree.OpsForMapKey("c")
	if len(ops) != 0 {
		t.Errorf("expected 0 ops for key 'c', got %d", len(ops))
	}
}

func TestOpTreeDeleteAndGet(t *testing.T) {
	tree := NewOpTree()
	op := Op{ID: types.OpId{Counter: 1}, Key: Key{Kind: KeyMap, MapKey: "a"}, Action: ActionSet}
	tree.Insert(op)

	got, ok := tree.Get(Key{Kind: KeyMap, MapKey: "a"}, types.OpId{Counter: 1})
	if !ok {
		t.Fatal("expected to find op")
	}
	if got.ID.Counter != 1 {
		t.Errorf("wrong op: %v", got)
	}

	_, ok = tree.Delete(Key{Kind: KeyMap, MapKey: "a"}, types.OpId{Counter: 1})
	if !ok {
		t.Fatal("expected delete to succeed")
	}
	if tree.Len() != 0 {
		t.Errorf("expected empty tree, got %d", tree.Len())
	}
}

func TestOpTreeVisibleOps(t *testing.T) {
	tree := NewOpTree()
	// op1 has a successor → not visible
	tree.Insert(Op{
		ID: types.OpId{Counter: 1}, Key: Key{Kind: KeyMap, MapKey: "a"},
		Action: ActionSet, Succ: []types.OpId{{Counter: 2}},
	})
	// op2 has no successors → visible
	tree.Insert(Op{
		ID: types.OpId{Counter: 2}, Key: Key{Kind: KeyMap, MapKey: "a"},
		Action: ActionSet,
	})
	// op3 is a delete → not visible
	tree.Insert(Op{
		ID: types.OpId{Counter: 3}, Key: Key{Kind: KeyMap, MapKey: "b"},
		Action: ActionDelete,
	})

	visible := tree.VisibleOps()
	if len(visible) != 1 {
		t.Fatalf("expected 1 visible op, got %d", len(visible))
	}
	if visible[0].ID.Counter != 2 {
		t.Errorf("wrong visible op: %v", visible[0])
	}

	visKey := tree.VisibleOpsForMapKey("a")
	if len(visKey) != 1 || visKey[0].ID.Counter != 2 {
		t.Errorf("wrong visible ops for key 'a': %v", visKey)
	}
}

func TestOpTreeSeqOps(t *testing.T) {
	tree := NewOpTree()
	elem1 := types.OpId{Counter: 1, ActorIdx: 0}
	elem2 := types.OpId{Counter: 2, ActorIdx: 0}

	tree.Insert(Op{
		ID: types.OpId{Counter: 3}, Key: Key{Kind: KeySeq, ElemID: elem1},
		Action: ActionSet, Insert: true,
	})
	tree.Insert(Op{
		ID: types.OpId{Counter: 4}, Key: Key{Kind: KeySeq, ElemID: elem2},
		Action: ActionSet, Insert: true,
	})

	ops := tree.OpsForSeqKey(elem1)
	if len(ops) != 1 || ops[0].ID.Counter != 3 {
		t.Errorf("wrong ops for elem1: %v", ops)
	}
}
