package automerge

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func testdataDir() string {
	return filepath.Join("testdata")
}

func loadFixture(t *testing.T, name string) *Doc {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(testdataDir(), name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	doc, err := Load(data)
	if err != nil {
		t.Fatalf("loading fixture %s: %v", name, err)
	}
	return doc
}

// TestCrossImplScalars verifies all scalar types match Rust output.
func TestCrossImplScalars(t *testing.T) {
	doc := loadFixture(t, "scalars.automerge")

	tests := []struct {
		key   string
		check func(t *testing.T, v Value)
	}{
		{"str", func(t *testing.T, v Value) {
			if v.Scalar.Str() != "hello world" {
				t.Errorf("str: got %q", v.Scalar.Str())
			}
		}},
		{"int", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeInt || v.Scalar.Int() != -42 {
				t.Errorf("int: got %v (type %s)", v.Scalar.Int(), v.Scalar.Type())
			}
		}},
		{"uint", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeUint || v.Scalar.Uint() != math.MaxUint64 {
				t.Errorf("uint: got %v", v.Scalar.Uint())
			}
		}},
		{"float", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeFloat64 || math.Abs(v.Scalar.Float64()-3.141592653589793) > 1e-15 {
				t.Errorf("float: got %v", v.Scalar.Float64())
			}
		}},
		{"true", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeTrue {
				t.Errorf("true: got type %s", v.Scalar.Type())
			}
		}},
		{"false", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeFalse {
				t.Errorf("false: got type %s", v.Scalar.Type())
			}
		}},
		{"null", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeNull {
				t.Errorf("null: got type %s", v.Scalar.Type())
			}
		}},
		{"bytes", func(t *testing.T, v Value) {
			expected := []byte{0xDE, 0xAD, 0xBE, 0xEF}
			if v.Scalar.Type() != ScalarTypeBytes || !bytes.Equal(v.Scalar.Bytes(), expected) {
				t.Errorf("bytes: got %x", v.Scalar.Bytes())
			}
		}},
		{"timestamp", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeTimestamp || v.Scalar.Timestamp() != -1000000 {
				t.Errorf("timestamp: got %v", v.Scalar.Timestamp())
			}
		}},
		{"counter", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeCounter || v.Scalar.Counter() != 100 {
				t.Errorf("counter: got %v", v.Scalar.Counter())
			}
		}},
		{"empty_str", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeString || v.Scalar.Str() != "" {
				t.Errorf("empty_str: got %q (type %s)", v.Scalar.Str(), v.Scalar.Type())
			}
		}},
		{"zero_int", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeInt || v.Scalar.Int() != 0 {
				t.Errorf("zero_int: got %v", v.Scalar.Int())
			}
		}},
		{"zero_uint", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeUint || v.Scalar.Uint() != 0 {
				t.Errorf("zero_uint: got %v", v.Scalar.Uint())
			}
		}},
		{"zero_float", func(t *testing.T, v Value) {
			if v.Scalar.Type() != ScalarTypeFloat64 || v.Scalar.Float64() != 0.0 {
				t.Errorf("zero_float: got %v", v.Scalar.Float64())
			}
		}},
		{"unicode", func(t *testing.T, v Value) {
			expected := "Hello 🌍 café résumé"
			if v.Scalar.Str() != expected {
				t.Errorf("unicode: got %q, want %q", v.Scalar.Str(), expected)
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			val, _, err := doc.Get(Root, MapProp(tt.key))
			if err != nil {
				t.Fatalf("Get(%q): %v", tt.key, err)
			}
			tt.check(t, val)
		})
	}

	// Verify all expected keys are present
	keys := doc.Keys(Root)
	if len(keys) != 15 {
		t.Errorf("expected 15 keys, got %d: %v", len(keys), keys)
	}
}

// TestCrossImplNestedObjects verifies nested maps and lists.
func TestCrossImplNestedObjects(t *testing.T) {
	doc := loadFixture(t, "nested_objects.automerge")

	keys := doc.Keys(Root)
	if len(keys) != 2 {
		t.Fatalf("expected 2 root keys, got %v", keys)
	}

	// config.debug = true
	configId := findObjId(doc, Root, "config")
	if configId.IsRoot() {
		t.Fatal("config not found")
	}

	debugVal, _, err := doc.Get(configId, MapProp("debug"))
	if err != nil {
		t.Fatalf("Get config.debug: %v", err)
	}
	if debugVal.Scalar.Type() != ScalarTypeTrue {
		t.Errorf("config.debug: expected true, got %s", debugVal.Scalar.Type())
	}

	// config.port = 8080
	portVal, _, err := doc.Get(configId, MapProp("port"))
	if err != nil {
		t.Fatalf("Get config.port: %v", err)
	}
	if portVal.Scalar.Int() != 8080 {
		t.Errorf("config.port: expected 8080, got %d", portVal.Scalar.Int())
	}

	// config.nested.deep = 42
	nestedId := findObjId(doc, configId, "nested")
	if nestedId.IsRoot() {
		t.Fatal("nested not found")
	}
	deepVal, _, err := doc.Get(nestedId, MapProp("deep"))
	if err != nil {
		t.Fatalf("Get config.nested.deep: %v", err)
	}
	if deepVal.Scalar.Int() != 42 {
		t.Errorf("config.nested.deep: expected 42, got %d", deepVal.Scalar.Int())
	}

	// items list has 5 elements
	itemsId := findObjId(doc, Root, "items")
	if doc.Length(itemsId) != 5 {
		t.Fatalf("items length: expected 5, got %d", doc.Length(itemsId))
	}

	// items[0] = "first"
	v0, _, _ := doc.Get(itemsId, SeqProp(0))
	if v0.Scalar.Str() != "first" {
		t.Errorf("items[0]: expected 'first', got %q", v0.Scalar.Str())
	}

	// items[1] = 2
	v1, _, _ := doc.Get(itemsId, SeqProp(1))
	if v1.Scalar.Int() != 2 {
		t.Errorf("items[1]: expected 2, got %d", v1.Scalar.Int())
	}

	// items[2] = true
	v2, _, _ := doc.Get(itemsId, SeqProp(2))
	if v2.Scalar.Type() != ScalarTypeTrue {
		t.Errorf("items[2]: expected true, got %s", v2.Scalar.Type())
	}

	// items[3] = map
	v3, _, _ := doc.Get(itemsId, SeqProp(3))
	if !v3.IsObject || v3.ObjType != ObjTypeMap {
		t.Fatalf("items[3]: expected map object, got %v", v3)
	}

	// items[4] = list
	v4, _, _ := doc.Get(itemsId, SeqProp(4))
	if !v4.IsObject || v4.ObjType != ObjTypeList {
		t.Fatalf("items[4]: expected list object, got %v", v4)
	}
}

// TestCrossImplListOperations verifies list insert/delete/splice.
func TestCrossImplListOperations(t *testing.T) {
	doc := loadFixture(t, "list_operations.automerge")

	listId := findObjId(doc, Root, "list")
	if listId.IsRoot() {
		t.Fatal("list not found")
	}

	// Final state: [a, X, Y, Z, e]
	expected := []string{"a", "X", "Y", "Z", "e"}
	if doc.Length(listId) != uint64(len(expected)) {
		t.Fatalf("list length: expected %d, got %d", len(expected), doc.Length(listId))
	}

	for i, want := range expected {
		val, _, err := doc.Get(listId, SeqProp(uint64(i)))
		if err != nil {
			t.Fatalf("Get list[%d]: %v", i, err)
		}
		if val.Scalar.Str() != want {
			t.Errorf("list[%d]: expected %q, got %q", i, want, val.Scalar.Str())
		}
	}
}

// TestCrossImplText verifies text splice operations.
func TestCrossImplText(t *testing.T) {
	doc := loadFixture(t, "text.automerge")

	textId := findObjId(doc, Root, "text")
	if textId.IsRoot() {
		t.Fatal("text not found")
	}

	text, err := doc.Text(textId)
	if err != nil {
		t.Fatalf("Text: %v", err)
	}
	if text != "Hello Go" {
		t.Errorf("text: expected %q, got %q", "Hello Go", text)
	}
}

// TestCrossImplCounter verifies counter increment operations.
func TestCrossImplCounter(t *testing.T) {
	doc := loadFixture(t, "counter.automerge")

	// counter(0) + 5 - 2 + 10 = 13
	val, _, err := doc.Get(Root, MapProp("count"))
	if err != nil {
		t.Fatalf("Get count: %v", err)
	}
	if val.Scalar.Type() != ScalarTypeCounter {
		t.Fatalf("count: expected counter type, got %s", val.Scalar.Type())
	}
	if val.Scalar.Counter() != 13 {
		t.Errorf("count: expected 13, got %d", val.Scalar.Counter())
	}
}

// TestCrossImplConcurrentEdits verifies merge with conflicts.
func TestCrossImplConcurrentEdits(t *testing.T) {
	doc := loadFixture(t, "concurrent_edits.automerge")

	// x = 1 (from initial)
	val, _, err := doc.Get(Root, MapProp("x"))
	if err != nil {
		t.Fatalf("Get x: %v", err)
	}
	if val.Scalar.Int() != 1 {
		t.Errorf("x: expected 1, got %d", val.Scalar.Int())
	}

	// only1 = "hello" (from actor1)
	val, _, err = doc.Get(Root, MapProp("only1"))
	if err != nil {
		t.Fatalf("Get only1: %v", err)
	}
	if val.Scalar.Str() != "hello" {
		t.Errorf("only1: expected 'hello', got %q", val.Scalar.Str())
	}

	// only2 = "world" (from actor2)
	val, _, err = doc.Get(Root, MapProp("only2"))
	if err != nil {
		t.Fatalf("Get only2: %v", err)
	}
	if val.Scalar.Str() != "world" {
		t.Errorf("only2: expected 'world', got %q", val.Scalar.Str())
	}

	// shared: should have 2 conflicting values
	allVals, err := doc.GetAll(Root, MapProp("shared"))
	if err != nil {
		t.Fatalf("GetAll shared: %v", err)
	}
	if len(allVals) != 2 {
		t.Fatalf("shared: expected 2 conflicting values, got %d", len(allVals))
	}

	// Winner should be "from-actor2" (actor2 ID 2222... > actor1 ID 1111...)
	winner, _, err := doc.Get(Root, MapProp("shared"))
	if err != nil {
		t.Fatalf("Get shared: %v", err)
	}
	if winner.Scalar.Str() != "from-actor2" {
		t.Errorf("shared winner: expected 'from-actor2', got %q", winner.Scalar.Str())
	}
}

// TestCrossImplDeleteOperations verifies delete on maps.
func TestCrossImplDeleteOperations(t *testing.T) {
	doc := loadFixture(t, "delete_operations.automerge")

	keys := doc.Keys(Root)
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %v", keys)
	}
	if keys[0] != "keep" {
		t.Errorf("expected key 'keep', got %q", keys[0])
	}

	val, _, err := doc.Get(Root, MapProp("keep"))
	if err != nil {
		t.Fatalf("Get keep: %v", err)
	}
	if val.Scalar.Str() != "stays" {
		t.Errorf("keep: expected 'stays', got %q", val.Scalar.Str())
	}

	_, _, err = doc.Get(Root, MapProp("remove"))
	if err != ErrNotFound {
		t.Errorf("remove: expected ErrNotFound, got %v", err)
	}
	_, _, err = doc.Get(Root, MapProp("also_remove"))
	if err != ErrNotFound {
		t.Errorf("also_remove: expected ErrNotFound, got %v", err)
	}
}

// TestCrossImplMultipleChanges verifies many sequential changes.
func TestCrossImplMultipleChanges(t *testing.T) {
	doc := loadFixture(t, "multiple_changes.automerge")

	val, _, err := doc.Get(Root, MapProp("step"))
	if err != nil {
		t.Fatalf("Get step: %v", err)
	}
	if val.Scalar.Int() != 9 {
		t.Errorf("step: expected 9, got %d", val.Scalar.Int())
	}

	keys := doc.Keys(Root)
	if len(keys) != 11 {
		t.Errorf("expected 11 keys, got %d: %v", len(keys), keys)
	}

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key_%d", i)
		val, _, err := doc.Get(Root, MapProp(key))
		if err != nil {
			t.Errorf("Get %s: %v", key, err)
			continue
		}
		if val.Scalar.Int() != int64(i) {
			t.Errorf("%s: expected %d, got %d", key, i, val.Scalar.Int())
		}
	}
}

// TestCrossImplEmptyDoc verifies an empty document.
func TestCrossImplEmptyDoc(t *testing.T) {
	doc := loadFixture(t, "empty.automerge")

	keys := doc.Keys(Root)
	if len(keys) != 0 {
		t.Errorf("expected 0 keys, got %v", keys)
	}
	if doc.opSet.Len() != 0 {
		t.Errorf("expected 0 ops, got %d", doc.opSet.Len())
	}
}

// TestCrossImplLargeText verifies a larger text object.
func TestCrossImplLargeText(t *testing.T) {
	doc := loadFixture(t, "large_text.automerge")

	contentId := findObjId(doc, Root, "content")
	if contentId.IsRoot() {
		t.Fatal("content not found")
	}

	text, err := doc.Text(contentId)
	if err != nil {
		t.Fatalf("Text: %v", err)
	}

	msg := "The quick brown fox jumps over the lazy dog. "
	expected := ""
	for i := 0; i < 10; i++ {
		expected += msg
	}
	if text != expected {
		t.Errorf("large_text: got %d chars, expected %d", len(text), len(expected))
	}
}

// TestCrossImplRoundTrip loads each fixture, saves, reloads, and verifies values match.
func TestCrossImplRoundTrip(t *testing.T) {
	fixtures := []string{
		"scalars.automerge",
		"nested_objects.automerge",
		"list_operations.automerge",
		"text.automerge",
		"counter.automerge",
		"concurrent_edits.automerge",
		"delete_operations.automerge",
		"multiple_changes.automerge",
		"empty.automerge",
		"large_text.automerge",
	}

	for _, name := range fixtures {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(testdataDir(), name))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			doc1, err := Load(data)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}

			saved, err := doc1.Save()
			if err != nil {
				t.Fatalf("Save: %v", err)
			}

			if !bytes.HasPrefix(saved, []byte{0x85, 0x6f, 0x4a, 0x83}) {
				t.Fatalf("missing magic bytes: %x", saved[:min(4, len(saved))])
			}

			doc2, err := Load(saved)
			if err != nil {
				t.Fatalf("Load round-trip: %v", err)
			}

			if doc1.opSet.Len() != doc2.opSet.Len() {
				t.Errorf("ops: %d vs %d", doc1.opSet.Len(), doc2.opSet.Len())
			}

			keys1 := doc1.Keys(Root)
			keys2 := doc2.Keys(Root)
			if len(keys1) != len(keys2) {
				t.Errorf("keys: %v vs %v", keys1, keys2)
			}

			heads1 := doc1.Heads()
			heads2 := doc2.Heads()
			if len(heads1) != len(heads2) {
				t.Errorf("heads: %d vs %d", len(heads1), len(heads2))
			}
		})
	}
}

// TestCrossImplDoubleRoundTrip does Load -> Save -> Load -> Save and verifies byte stability.
func TestCrossImplDoubleRoundTrip(t *testing.T) {
	fixtures := []string{
		"scalars.automerge",
		"nested_objects.automerge",
		"list_operations.automerge",
		"text.automerge",
		"counter.automerge",
		"delete_operations.automerge",
		"multiple_changes.automerge",
		"large_text.automerge",
	}

	for _, name := range fixtures {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(testdataDir(), name))
			if err != nil {
				t.Fatalf("read: %v", err)
			}

			doc1, err := Load(data)
			if err != nil {
				t.Fatalf("Load 1: %v", err)
			}
			saved1, err := doc1.Save()
			if err != nil {
				t.Fatalf("Save 1: %v", err)
			}

			doc2, err := Load(saved1)
			if err != nil {
				t.Fatalf("Load 2: %v", err)
			}
			saved2, err := doc2.Save()
			if err != nil {
				t.Fatalf("Save 2: %v", err)
			}

			if !bytes.Equal(saved1, saved2) {
				t.Errorf("double round-trip produced different bytes: %d vs %d bytes", len(saved1), len(saved2))
			}
		})
	}
}

// TestCrossImplGoCreateRoundTrip creates docs in Go and verifies round-trip.
func TestCrossImplGoCreateRoundTrip(t *testing.T) {
	t.Run("scalars", func(t *testing.T) {
		doc := New()
		if err := doc.Put(Root, "str", NewStr("hello")); err != nil {
			t.Fatal(err)
		}
		if err := doc.Put(Root, "int", NewInt(-99)); err != nil {
			t.Fatal(err)
		}
		if err := doc.Put(Root, "uint", NewUint(42)); err != nil {
			t.Fatal(err)
		}
		if err := doc.Put(Root, "float", NewFloat64(2.718)); err != nil {
			t.Fatal(err)
		}
		if err := doc.Put(Root, "bool", NewBool(true)); err != nil {
			t.Fatal(err)
		}
		if err := doc.Put(Root, "bytes", NewBytes([]byte{1, 2, 3})); err != nil {
			t.Fatal(err)
		}
		doc.Commit("test", 1000)

		data, err := doc.Save()
		if err != nil {
			t.Fatalf("Save: %v", err)
		}

		doc2, err := Load(data)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		val, _, _ := doc2.Get(Root, MapProp("str"))
		if val.Scalar.Str() != "hello" {
			t.Errorf("str: got %q", val.Scalar.Str())
		}
		val, _, _ = doc2.Get(Root, MapProp("int"))
		if val.Scalar.Int() != -99 {
			t.Errorf("int: got %d", val.Scalar.Int())
		}
		val, _, _ = doc2.Get(Root, MapProp("uint"))
		if val.Scalar.Uint() != 42 {
			t.Errorf("uint: got %d", val.Scalar.Uint())
		}
	})

	t.Run("nested_objects", func(t *testing.T) {
		doc := New()
		mapId, _ := doc.PutObject(Root, "config", ObjTypeMap)
		if err := doc.Put(mapId, "x", NewInt(1)); err != nil {
			t.Fatal(err)
		}
		listId, _ := doc.PutObject(Root, "items", ObjTypeList)
		if err := doc.Insert(listId, 0, NewStr("a")); err != nil {
			t.Fatal(err)
		}
		if err := doc.Insert(listId, 1, NewStr("b")); err != nil {
			t.Fatal(err)
		}
		doc.Commit("test", 1000)

		data, _ := doc.Save()
		doc2, err := Load(data)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		configId := findObjId(doc2, Root, "config")
		v, _, _ := doc2.Get(configId, MapProp("x"))
		if v.Scalar.Int() != 1 {
			t.Errorf("config.x: got %d", v.Scalar.Int())
		}

		itemsId := findObjId(doc2, Root, "items")
		if doc2.Length(itemsId) != 2 {
			t.Errorf("items length: got %d", doc2.Length(itemsId))
		}
	})

	t.Run("text", func(t *testing.T) {
		doc := New()
		textId, _ := doc.PutObject(Root, "notes", ObjTypeText)
		if err := doc.SpliceText(textId, 0, 0, "Hello World"); err != nil {
			t.Fatal(err)
		}
		doc.Commit("test", 1000)

		data, _ := doc.Save()
		doc2, err := Load(data)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		notesId := findObjId(doc2, Root, "notes")
		text, _ := doc2.Text(notesId)
		if text != "Hello World" {
			t.Errorf("text: got %q", text)
		}
	})
}

// TestCrossImplRustCLIVerify uses the Rust CLI to verify Go-saved output is valid automerge.
func TestCrossImplRustCLIVerify(t *testing.T) {
	cliPath := os.Getenv("AUTOMERGE_CLI")
	if cliPath == "" {
		var err error
		cliPath, err = filepath.Abs(filepath.Join("..", "..", "automerge", "rust", "target", "release", "automerge"))
		if err != nil {
			t.Skipf("could not resolve CLI path: %v", err)
		}
	}
	if _, err := os.Stat(cliPath); err != nil {
		t.Skipf("Rust CLI not found at %s (build with: cargo build --release -p automerge-cli)", cliPath)
	}

	fixtures := []string{
		"scalars.automerge",
		"nested_objects.automerge",
		"list_operations.automerge",
		"text.automerge",
		"counter.automerge",
		"delete_operations.automerge",
		"multiple_changes.automerge",
		"large_text.automerge",
	}

	for _, name := range fixtures {
		t.Run(name, func(t *testing.T) {
			// Load Rust fixture in Go, save it back out
			data, err := os.ReadFile(filepath.Join(testdataDir(), name))
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			doc, err := Load(data)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}

			saved, err := doc.Save()
			if err != nil {
				t.Fatalf("Save: %v", err)
			}

			// Write Go-saved bytes to temp file
			tmpFile := filepath.Join(t.TempDir(), name)
			if err := os.WriteFile(tmpFile, saved, 0644); err != nil {
				t.Fatalf("write: %v", err)
			}

			// Use Rust CLI to export — validates the binary format
			cmd := exec.Command(cliPath, "export", tmpFile)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("Rust CLI failed to read Go-saved file:\n%s\nerror: %v", string(out), err)
			}

			if len(out) == 0 {
				t.Error("Rust CLI produced empty export")
			}
		})
	}

	// Also test Go-created documents
	t.Run("go_created", func(t *testing.T) {
		doc := New()
		if err := doc.Put(Root, "name", NewStr("test")); err != nil {
			t.Fatal(err)
		}
		if err := doc.Put(Root, "value", NewInt(42)); err != nil {
			t.Fatal(err)
		}
		mapId, _ := doc.PutObject(Root, "nested", ObjTypeMap)
		if err := doc.Put(mapId, "x", NewBool(true)); err != nil {
			t.Fatal(err)
		}
		listId, _ := doc.PutObject(Root, "items", ObjTypeList)
		if err := doc.Insert(listId, 0, NewStr("a")); err != nil {
			t.Fatal(err)
		}
		if err := doc.Insert(listId, 1, NewStr("b")); err != nil {
			t.Fatal(err)
		}
		textId, _ := doc.PutObject(Root, "notes", ObjTypeText)
		if err := doc.SpliceText(textId, 0, 0, "Hello World"); err != nil {
			t.Fatal(err)
		}
		doc.Commit("test", 1000)

		saved, err := doc.Save()
		if err != nil {
			t.Fatalf("Save: %v", err)
		}

		tmpFile := filepath.Join(t.TempDir(), "go_created.automerge")
		if err := os.WriteFile(tmpFile, saved, 0644); err != nil {
			t.Fatalf("write: %v", err)
		}

		cmd := exec.Command(cliPath, "export", tmpFile)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Rust CLI failed to read Go-created file:\n%s\nerror: %v", string(out), err)
		}

		if len(out) == 0 {
			t.Error("Rust CLI produced empty export for Go-created doc")
		}
		t.Logf("Rust export of Go-created doc: %s", string(out))
	})
}
