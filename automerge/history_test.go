package automerge

import (
	"testing"
)

func TestGetAt(t *testing.T) {
	doc := New()
	doc.Put(Root, "x", NewInt(1))
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	doc.Put(Root, "x", NewInt(2))
	doc.Put(Root, "y", NewStr("hello"))
	doc.Commit("v2", 2000)

	// At v1: x=1, y doesn't exist
	val, _, err := doc.GetAt(Root, MapProp("x"), headsV1)
	if err != nil {
		t.Fatalf("GetAt x at v1: %v", err)
	}
	if val.Scalar.Int() != 1 {
		t.Errorf("expected x=1 at v1, got %s", val)
	}

	_, _, err = doc.GetAt(Root, MapProp("y"), headsV1)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for y at v1, got %v", err)
	}

	// At current heads: x=2, y=hello
	val, _, err = doc.GetAt(Root, MapProp("x"), doc.Heads())
	if err != nil {
		t.Fatalf("GetAt x at v2: %v", err)
	}
	if val.Scalar.Int() != 2 {
		t.Errorf("expected x=2 at v2, got %s", val)
	}

	val, _, err = doc.GetAt(Root, MapProp("y"), doc.Heads())
	if err != nil {
		t.Fatalf("GetAt y at v2: %v", err)
	}
	if val.Scalar.Str() != "hello" {
		t.Errorf("expected y=hello at v2, got %s", val)
	}
}

func TestKeysAt(t *testing.T) {
	doc := New()
	doc.Put(Root, "a", NewInt(1))
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	doc.Put(Root, "b", NewInt(2))
	doc.Commit("v2", 2000)

	keys := doc.KeysAt(Root, headsV1)
	if len(keys) != 1 || keys[0] != "a" {
		t.Errorf("expected [a] at v1, got %v", keys)
	}

	keys = doc.KeysAt(Root, doc.Heads())
	if len(keys) != 2 {
		t.Errorf("expected 2 keys at v2, got %v", keys)
	}
}

func TestTextAt(t *testing.T) {
	doc := New()
	textObj, _ := doc.PutObject(Root, "notes", ObjTypeText)
	doc.SpliceText(textObj, 0, 0, "hello")
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	doc.SpliceText(textObj, 5, 0, " world")
	doc.Commit("v2", 2000)

	text, err := doc.TextAt(textObj, headsV1)
	if err != nil {
		t.Fatalf("TextAt v1: %v", err)
	}
	if text != "hello" {
		t.Errorf("expected 'hello' at v1, got %q", text)
	}

	text, err = doc.TextAt(textObj, doc.Heads())
	if err != nil {
		t.Fatalf("TextAt v2: %v", err)
	}
	if text != "hello world" {
		t.Errorf("expected 'hello world' at v2, got %q", text)
	}
}

func TestLengthAt(t *testing.T) {
	doc := New()
	listObj, _ := doc.PutObject(Root, "items", ObjTypeList)
	doc.Insert(listObj, 0, NewInt(1))
	doc.Insert(listObj, 1, NewInt(2))
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	doc.Insert(listObj, 2, NewInt(3))
	doc.Commit("v2", 2000)

	if l := doc.LengthAt(listObj, headsV1); l != 2 {
		t.Errorf("expected length 2 at v1, got %d", l)
	}
	if l := doc.LengthAt(listObj, doc.Heads()); l != 3 {
		t.Errorf("expected length 3 at v2, got %d", l)
	}
}

func TestMapRangeAt(t *testing.T) {
	doc := New()
	doc.Put(Root, "x", NewInt(1))
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	doc.Put(Root, "y", NewInt(2))
	doc.Commit("v2", 2000)

	count := 0
	for k, v := range doc.MapRangeAt(Root, headsV1) {
		if k != "x" || v.Scalar.Int() != 1 {
			t.Errorf("unexpected entry: %s=%s", k, v)
		}
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 entry at v1, got %d", count)
	}
}

func TestListItemsAt(t *testing.T) {
	doc := New()
	listObj, _ := doc.PutObject(Root, "items", ObjTypeList)
	doc.Insert(listObj, 0, NewStr("a"))
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	doc.Insert(listObj, 1, NewStr("b"))
	doc.Commit("v2", 2000)

	var items []string
	for _, v := range doc.ListItemsAt(listObj, headsV1) {
		items = append(items, v.Scalar.Str())
	}
	if len(items) != 1 || items[0] != "a" {
		t.Errorf("expected [a] at v1, got %v", items)
	}
}

func TestGetAllAt(t *testing.T) {
	// Create a conflict at key "x" using two forked docs
	doc1 := New()
	if err := doc1.Put(Root, "x", NewInt(1)); err != nil {
		t.Fatal(err)
	}
	doc1.Commit("v1", 1000)
	headsV1 := doc1.Heads()

	doc2 := doc1.Fork()

	if err := doc1.Put(Root, "x", NewInt(10)); err != nil {
		t.Fatal(err)
	}
	doc1.Commit("d1", 2000)

	if err := doc2.Put(Root, "x", NewInt(20)); err != nil {
		t.Fatal(err)
	}
	doc2.Commit("d2", 2000)

	if err := doc1.Merge(doc2); err != nil {
		t.Fatal(err)
	}

	// At v1: only one value
	vals, err := doc1.GetAllAt(Root, MapProp("x"), headsV1)
	if err != nil {
		t.Fatalf("GetAllAt v1: %v", err)
	}
	if len(vals) != 1 {
		t.Errorf("expected 1 value at v1, got %d", len(vals))
	}

	// At current heads: two conflicting values
	vals, err = doc1.GetAllAt(Root, MapProp("x"), doc1.Heads())
	if err != nil {
		t.Fatalf("GetAllAt current: %v", err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 values (conflict) at current, got %d", len(vals))
	}
}

func TestDeletedKeyAt(t *testing.T) {
	doc := New()
	doc.Put(Root, "x", NewInt(1))
	doc.Put(Root, "y", NewInt(2))
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	if err := doc.Delete(Root, MapProp("y")); err != nil {
		t.Fatal(err)
	}
	doc.Commit("v2", 2000)

	keys := doc.KeysAt(Root, headsV1)
	if len(keys) != 2 {
		t.Errorf("expected 2 keys at v1, got %v", keys)
	}

	keys = doc.KeysAt(Root, doc.Heads())
	if len(keys) != 1 || keys[0] != "x" {
		t.Errorf("expected [x] at v2, got %v", keys)
	}
}
