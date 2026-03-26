package automerge

import (
	"testing"
)

func TestDiffMapPut(t *testing.T) {
	doc := New()
	doc.Put(Root, "x", NewInt(1))
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	doc.Put(Root, "y", NewInt(2))
	doc.Put(Root, "x", NewInt(10))
	doc.Commit("v2", 2000)
	headsV2 := doc.Heads()

	patches := doc.Diff(headsV1, headsV2)
	if len(patches) == 0 {
		t.Fatal("expected patches")
	}

	// Should have a PutMap for y (new key) and x (changed value)
	foundY := false
	foundX := false
	for _, p := range patches {
		if p.Action == PatchPutMap {
			if p.Key == "y" {
				foundY = true
			}
			if p.Key == "x" {
				foundX = true
			}
		}
	}
	if !foundY {
		t.Error("expected PutMap patch for key y")
	}
	if !foundX {
		t.Error("expected PutMap patch for key x")
	}
}

func TestDiffMapDelete(t *testing.T) {
	doc := New()
	doc.Put(Root, "x", NewInt(1))
	doc.Put(Root, "y", NewInt(2))
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	if err := doc.Delete(Root, MapProp("y")); err != nil {
		t.Fatal(err)
	}
	doc.Commit("v2", 2000)
	headsV2 := doc.Heads()

	patches := doc.Diff(headsV1, headsV2)
	foundDelete := false
	for _, p := range patches {
		if p.Action == PatchDeleteMap && p.Key == "y" {
			foundDelete = true
		}
	}
	if !foundDelete {
		t.Error("expected DeleteMap patch for key y")
	}
}

func TestDiffText(t *testing.T) {
	doc := New()
	textObj, _ := doc.PutObject(Root, "notes", ObjTypeText)
	doc.SpliceText(textObj, 0, 0, "hello")
	doc.Commit("v1", 1000)
	headsV1 := doc.Heads()

	doc.SpliceText(textObj, 5, 0, " world")
	doc.Commit("v2", 2000)
	headsV2 := doc.Heads()

	patches := doc.Diff(headsV1, headsV2)
	foundSplice := false
	for _, p := range patches {
		if p.Action == PatchSpliceText && p.Text == " world" {
			foundSplice = true
		}
	}
	if !foundSplice {
		t.Error("expected SpliceText patch for ' world'")
	}
}

func TestDiffEmpty(t *testing.T) {
	doc := New()
	doc.Put(Root, "x", NewInt(1))
	doc.Commit("v1", 1000)

	// Diff same heads should be empty
	patches := doc.Diff(doc.Heads(), doc.Heads())
	if len(patches) != 0 {
		t.Errorf("expected no patches for same heads, got %d", len(patches))
	}
}

func TestPatchLog(t *testing.T) {
	pl := NewPatchLog()
	if len(pl.Patches()) != 0 {
		t.Error("expected empty patch log")
	}

	pl.patches = append(pl.patches, Patch{Action: PatchPutMap, Key: "x"})
	if len(pl.Patches()) != 1 {
		t.Error("expected 1 patch")
	}

	pl.Clear()
	if len(pl.Patches()) != 0 {
		t.Error("expected empty after clear")
	}
}
