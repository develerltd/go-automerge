package automerge

import (
	"testing"
)

func TestMarkBasic(t *testing.T) {
	doc := New()
	textObj, _ := doc.PutObject(Root, "text", ObjTypeText)
	doc.SpliceText(textObj, 0, 0, "hello world")
	doc.Commit("text", 1000)

	// Mark "hello" as bold
	err := doc.Mark(textObj, 0, 5, ExpandAfter, "bold", NewBool(true))
	if err != nil {
		t.Fatalf("Mark: %v", err)
	}
	doc.Commit("bold", 2000)

	marks, err := doc.Marks(textObj)
	if err != nil {
		t.Fatalf("Marks: %v", err)
	}
	if len(marks) == 0 {
		t.Fatal("expected at least one mark")
	}

	found := false
	for _, m := range marks {
		if m.Name == "bold" {
			found = true
			if m.Start != 0 || m.End != 5 {
				t.Errorf("expected mark [0,5), got [%d,%d)", m.Start, m.End)
			}
		}
	}
	if !found {
		t.Error("did not find bold mark")
	}
}

func TestMarkAtPosition(t *testing.T) {
	doc := New()
	textObj, _ := doc.PutObject(Root, "text", ObjTypeText)
	doc.SpliceText(textObj, 0, 0, "abcdef")
	doc.Commit("text", 1000)

	// Mark positions 2-4 as italic
	err := doc.Mark(textObj, 2, 4, ExpandNone, "italic", NewBool(true))
	if err != nil {
		t.Fatalf("Mark: %v", err)
	}
	doc.Commit("italic", 2000)

	// Position 1 should NOT have italic
	ms, err := doc.MarksAtPosition(textObj, 1)
	if err != nil {
		t.Fatalf("MarksAtPosition(1): %v", err)
	}
	if ms.Len() != 0 {
		t.Errorf("position 1 should have no marks, got %d", ms.Len())
	}

	// Position 2 should have italic
	ms, err = doc.MarksAtPosition(textObj, 2)
	if err != nil {
		t.Fatalf("MarksAtPosition(2): %v", err)
	}
	if ms.Len() == 0 {
		t.Error("position 2 should have italic mark")
	}
}

func TestMarksOnNonText(t *testing.T) {
	doc := New()
	listObj, _ := doc.PutObject(Root, "list", ObjTypeList)
	doc.Insert(listObj, 0, NewInt(1))
	doc.Commit("list", 1000)

	err := doc.Mark(listObj, 0, 1, ExpandNone, "test", NewBool(true))
	if err == nil {
		t.Error("expected error marking non-text object")
	}
}

func TestMarkSet(t *testing.T) {
	ms := NewMarkSet()
	if ms.Len() != 0 {
		t.Errorf("expected empty, got %d", ms.Len())
	}

	ms.marks["bold"] = NewBool(true)
	ms.marks["color"] = NewStr("red")

	if ms.Len() != 2 {
		t.Errorf("expected 2, got %d", ms.Len())
	}

	val, ok := ms.Get("bold")
	if !ok || !val.Bool() {
		t.Error("expected bold=true")
	}

	var names []string
	ms.Range(func(name string, _ ScalarValue) bool {
		names = append(names, name)
		return true
	})
	if len(names) != 2 || names[0] != "bold" || names[1] != "color" {
		t.Errorf("unexpected range order: %v", names)
	}
}
