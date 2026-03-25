package automerge

import (
	"testing"
)

func TestCursorBasic(t *testing.T) {
	doc := New()
	textObj, _ := doc.PutObject(Root, "text", ObjTypeText)
	doc.SpliceText(textObj, 0, 0, "hello")
	doc.Commit("text", 1000)

	// Get cursor at position 2
	cursor, err := doc.GetCursor(textObj, 2, MoveAfter)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}

	if cursor.Kind != CursorOp {
		t.Errorf("expected CursorOp, got %d", cursor.Kind)
	}

	// Resolve cursor
	pos, err := doc.GetCursorPosition(textObj, cursor)
	if err != nil {
		t.Fatalf("GetCursorPosition: %v", err)
	}
	if pos.Index != 2 {
		t.Errorf("expected position 2, got %d", pos.Index)
	}
}

func TestCursorStartEnd(t *testing.T) {
	doc := New()
	textObj, _ := doc.PutObject(Root, "text", ObjTypeText)
	doc.SpliceText(textObj, 0, 0, "abc")
	doc.Commit("text", 1000)

	pos, err := doc.GetCursorPosition(textObj, StartCursor())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if pos.Index != 0 || !pos.IsStart {
		t.Errorf("expected start at 0, got %d", pos.Index)
	}

	pos, err = doc.GetCursorPosition(textObj, EndCursor())
	if err != nil {
		t.Fatalf("End: %v", err)
	}
	if pos.Index != 3 || !pos.IsEnd {
		t.Errorf("expected end at 3, got %d", pos.Index)
	}
}

func TestCursorBytesRoundTrip(t *testing.T) {
	doc := New()
	textObj, _ := doc.PutObject(Root, "text", ObjTypeText)
	doc.SpliceText(textObj, 0, 0, "test")
	doc.Commit("text", 1000)

	cursor, err := doc.GetCursor(textObj, 1, MoveAfter)
	if err != nil {
		t.Fatalf("GetCursor: %v", err)
	}

	data := cursor.ToBytes()
	restored, err := CursorFromBytes(data)
	if err != nil {
		t.Fatalf("CursorFromBytes: %v", err)
	}

	if restored.Kind != cursor.Kind {
		t.Errorf("kind mismatch: %d vs %d", restored.Kind, cursor.Kind)
	}
	if restored.Counter != cursor.Counter {
		t.Errorf("counter mismatch: %d vs %d", restored.Counter, cursor.Counter)
	}
	if restored.Actor.Compare(cursor.Actor) != 0 {
		t.Error("actor mismatch")
	}
	if restored.MoveCursor != cursor.MoveCursor {
		t.Errorf("move mismatch: %d vs %d", restored.MoveCursor, cursor.MoveCursor)
	}
}

func TestCursorStringRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		cursor Cursor
	}{
		{"start", StartCursor()},
		{"end", EndCursor()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.cursor.String()
			restored, err := CursorFromString(s)
			if err != nil {
				t.Fatalf("CursorFromString(%q): %v", s, err)
			}
			if restored.Kind != tt.cursor.Kind {
				t.Errorf("kind mismatch")
			}
		})
	}
}

func TestCursorStartEndBytes(t *testing.T) {
	for _, c := range []Cursor{StartCursor(), EndCursor()} {
		data := c.ToBytes()
		restored, err := CursorFromBytes(data)
		if err != nil {
			t.Fatalf("CursorFromBytes: %v", err)
		}
		if restored.Kind != c.Kind {
			t.Errorf("kind mismatch for %s", c)
		}
	}
}

func TestCursorOnNonSequence(t *testing.T) {
	doc := New()
	doc.Put(Root, "x", NewInt(1))
	doc.Commit("v1", 1000)

	_, err := doc.GetCursor(Root, 0, MoveAfter)
	if err == nil {
		t.Error("expected error for cursor on map")
	}
}
