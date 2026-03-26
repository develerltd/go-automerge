package automerge

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixturesDir() string {
	return filepath.Join("..", "..", "automerge", "rust", "automerge", "tests", "fixtures")
}

func interopDir() string {
	return filepath.Join("..", "..", "automerge", "interop")
}

func TestLoadAllFixtures(t *testing.T) {
	dir := fixturesDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Skipf("fixtures dir not found: %v", err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".automerge") {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			doc, err := Load(data)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			t.Logf("loaded: actors=%d, heads=%d, ops=%d",
				len(doc.Actors()), len(doc.Heads()), doc.opSet.Len())
		})
	}
}

func TestExemplarValues(t *testing.T) {
	path := filepath.Join(interopDir(), "exemplar")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("exemplar not found: %v", err)
	}

	doc, err := Load(data)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	t.Logf("actors=%d, heads=%d, ops=%d, keys=%v",
		len(doc.Actors()), len(doc.Heads()), doc.opSet.Len(), doc.Keys(Root))

	// Test: bool = true
	val, _, err := doc.Get(Root, MapProp("bool"))
	if err != nil {
		t.Fatalf("Get bool: %v", err)
	}
	if val.Scalar.Type() != ScalarTypeTrue {
		t.Errorf("bool: expected true, got %s", val)
	}

	// Test: int = -4
	val, _, err = doc.Get(Root, MapProp("int"))
	if err != nil {
		t.Fatalf("Get int: %v", err)
	}
	if val.Scalar.Type() != ScalarTypeInt || val.Scalar.Int() != -4 {
		t.Errorf("int: expected -4, got %s", val)
	}

	// Test: uint = 18446744073709551615 (max uint64)
	val, _, err = doc.Get(Root, MapProp("uint"))
	if err != nil {
		t.Fatalf("Get uint: %v", err)
	}
	if val.Scalar.Type() != ScalarTypeUint || val.Scalar.Uint() != math.MaxUint64 {
		t.Errorf("uint: expected max uint64, got %s (type=%s)", val, val.Scalar.Type())
	}

	// Test: fp = 3.14159267
	val, _, err = doc.Get(Root, MapProp("fp"))
	if err != nil {
		t.Fatalf("Get fp: %v", err)
	}
	if val.Scalar.Type() != ScalarTypeFloat64 {
		t.Errorf("fp: expected float64, got type %s", val.Scalar.Type())
	} else if math.Abs(val.Scalar.Float64()-3.14159267) > 1e-8 {
		t.Errorf("fp: expected 3.14159267, got %v", val.Scalar.Float64())
	}

	// Test: counter = 5
	val, _, err = doc.Get(Root, MapProp("counter"))
	if err != nil {
		t.Fatalf("Get counter: %v", err)
	}
	if val.Scalar.Type() != ScalarTypeCounter || val.Scalar.Counter() != 5 {
		t.Errorf("counter: expected counter(5), got %s", val)
	}

	// Test: timestamp = -905182979000
	val, _, err = doc.Get(Root, MapProp("timestamp"))
	if err != nil {
		t.Fatalf("Get timestamp: %v", err)
	}
	if val.Scalar.Type() != ScalarTypeTimestamp || val.Scalar.Timestamp() != -905182979000 {
		t.Errorf("timestamp: expected -905182979000, got %s", val)
	}

	// Test: bytes = [0x85, 0x6f, 0x4a, 0x83]
	val, _, err = doc.Get(Root, MapProp("bytes"))
	if err != nil {
		t.Fatalf("Get bytes: %v", err)
	}
	expectedBytes := []byte{0x85, 0x6f, 0x4a, 0x83}
	if val.Scalar.Type() != ScalarTypeBytes {
		t.Errorf("bytes: expected bytes type, got %s", val.Scalar.Type())
	} else {
		got := val.Scalar.Bytes()
		if len(got) != len(expectedBytes) {
			t.Errorf("bytes: expected %x, got %x", expectedBytes, got)
		} else {
			for i := range got {
				if got[i] != expectedBytes[i] {
					t.Errorf("bytes: expected %x, got %x", expectedBytes, got)
					break
				}
			}
		}
	}

	// Test: title = "Hello 🇬🇧👨‍👨‍👧‍👦😀"
	val, _, err = doc.Get(Root, MapProp("title"))
	if err != nil {
		t.Fatalf("Get title: %v", err)
	}
	expectedTitle := "Hello 🇬🇧👨\u200d👨\u200d👧\u200d👦😀"
	if val.Scalar.Type() != ScalarTypeString || val.Scalar.Str() != expectedTitle {
		t.Errorf("title: expected %q, got %q (type=%s)", expectedTitle, val.Scalar.Str(), val.Scalar.Type())
	}

	// Test: location = "https://automerge.org/"
	val, _, err = doc.Get(Root, MapProp("location"))
	if err != nil {
		t.Fatalf("Get location: %v", err)
	}
	if val.Scalar.Type() != ScalarTypeString || val.Scalar.Str() != "https://automerge.org/" {
		t.Errorf("location: expected 'https://automerge.org/', got %s", val)
	}

	// Test: notes is a text object with content "🇬🇧👨‍👨‍👧‍👦😀"
	notesVal, _, err := doc.Get(Root, MapProp("notes"))
	if err != nil {
		t.Fatalf("Get notes: %v", err)
	}
	if !notesVal.IsObject || notesVal.ObjType != ObjTypeText {
		t.Errorf("notes: expected text object, got %s", notesVal)
	}
}

func TestNewDoc(t *testing.T) {
	doc := New()
	if len(doc.Actors()) != 1 {
		t.Fatalf("expected 1 actor, got %d", len(doc.Actors()))
	}
	if len(doc.Actors()[0]) != 16 {
		t.Fatalf("expected 16-byte actor ID, got %d bytes", len(doc.Actors()[0]))
	}
	if doc.opSet.Len() != 0 {
		t.Errorf("expected 0 ops, got %d", doc.opSet.Len())
	}
}

func TestPutAndGet(t *testing.T) {
	doc := New()

	// Put string
	if err := doc.Put(Root, "name", NewStr("Alice")); err != nil {
		t.Fatal(err)
	}
	val, _, err := doc.Get(Root, MapProp("name"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Scalar.Str() != "Alice" {
		t.Errorf("expected 'Alice', got %s", val)
	}

	// Put int
	if err := doc.Put(Root, "age", NewInt(30)); err != nil {
		t.Fatal(err)
	}
	val, _, err = doc.Get(Root, MapProp("age"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Scalar.Int() != 30 {
		t.Errorf("expected 30, got %d", val.Scalar.Int())
	}

	// Overwrite
	if err := doc.Put(Root, "name", NewStr("Bob")); err != nil {
		t.Fatal(err)
	}
	val, _, err = doc.Get(Root, MapProp("name"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Scalar.Str() != "Bob" {
		t.Errorf("expected 'Bob', got %s", val)
	}

	// Keys
	keys := doc.Keys(Root)
	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %v", keys)
	}
}

func TestPutObject(t *testing.T) {
	doc := New()

	// Create a map object
	mapId, err := doc.PutObject(Root, "config", ObjTypeMap)
	if err != nil {
		t.Fatal(err)
	}

	// Put values in nested map
	if err := doc.Put(mapId, "debug", NewBool(true)); err != nil {
		t.Fatal(err)
	}
	if err := doc.Put(mapId, "level", NewStr("info")); err != nil {
		t.Fatal(err)
	}

	// Read back
	val, _, err := doc.Get(mapId, MapProp("debug"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Scalar.Type() != ScalarTypeTrue {
		t.Errorf("expected true, got %s", val)
	}
	val, _, err = doc.Get(mapId, MapProp("level"))
	if err != nil {
		t.Fatal(err)
	}
	if val.Scalar.Str() != "info" {
		t.Errorf("expected 'info', got %s", val)
	}
}

func TestDelete(t *testing.T) {
	doc := New()

	if err := doc.Put(Root, "a", NewStr("hello")); err != nil {
		t.Fatal(err)
	}
	if err := doc.Put(Root, "b", NewStr("world")); err != nil {
		t.Fatal(err)
	}

	if err := doc.Delete(Root, MapProp("a")); err != nil {
		t.Fatal(err)
	}

	_, _, err := doc.Get(Root, MapProp("a"))
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	keys := doc.Keys(Root)
	if len(keys) != 1 || keys[0] != "b" {
		t.Errorf("expected [b], got %v", keys)
	}
}

func TestSaveAndLoad(t *testing.T) {
	doc := New()

	doc.Put(Root, "title", NewStr("Hello World"))
	doc.Put(Root, "count", NewInt(42))
	doc.Put(Root, "pi", NewFloat64(3.14159))
	doc.Put(Root, "flag", NewBool(true))
	doc.Put(Root, "data", NewBytes([]byte{0xDE, 0xAD}))
	doc.Commit("initial", 1000)

	data, err := doc.Save()
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify it starts with magic bytes
	if !bytes.HasPrefix(data, []byte{0x85, 0x6f, 0x4a, 0x83}) {
		t.Fatalf("missing magic bytes: %x", data[:4])
	}

	// Load it back
	doc2, err := Load(data)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Verify all values
	val, _, err := doc2.Get(Root, MapProp("title"))
	if err != nil {
		t.Fatalf("Get title: %v", err)
	}
	if val.Scalar.Str() != "Hello World" {
		t.Errorf("title: expected 'Hello World', got %s", val)
	}

	val, _, err = doc2.Get(Root, MapProp("count"))
	if err != nil {
		t.Fatalf("Get count: %v", err)
	}
	if val.Scalar.Int() != 42 {
		t.Errorf("count: expected 42, got %d", val.Scalar.Int())
	}

	val, _, err = doc2.Get(Root, MapProp("pi"))
	if err != nil {
		t.Fatalf("Get pi: %v", err)
	}
	if math.Abs(val.Scalar.Float64()-3.14159) > 1e-10 {
		t.Errorf("pi: expected 3.14159, got %v", val.Scalar.Float64())
	}

	val, _, err = doc2.Get(Root, MapProp("flag"))
	if err != nil {
		t.Fatalf("Get flag: %v", err)
	}
	if val.Scalar.Type() != ScalarTypeTrue {
		t.Errorf("flag: expected true, got %s", val)
	}

	val, _, err = doc2.Get(Root, MapProp("data"))
	if err != nil {
		t.Fatalf("Get data: %v", err)
	}
	if !bytes.Equal(val.Scalar.Bytes(), []byte{0xDE, 0xAD}) {
		t.Errorf("data: expected DEAD, got %x", val.Scalar.Bytes())
	}

	// Verify keys match
	keys1 := doc.Keys(Root)
	keys2 := doc2.Keys(Root)
	if len(keys1) != len(keys2) {
		t.Errorf("key count mismatch: %d vs %d", len(keys1), len(keys2))
	}
}

func TestSaveLoadNestedObjects(t *testing.T) {
	doc := New()

	mapId, err := doc.PutObject(Root, "config", ObjTypeMap)
	if err != nil {
		t.Fatal(err)
	}
	doc.Put(mapId, "debug", NewBool(true))
	doc.Put(mapId, "port", NewInt(8080))

	listId, err := doc.PutObject(Root, "items", ObjTypeList)
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Insert(listId, 0, NewStr("first")); err != nil {
		t.Fatal(err)
	}
	if err := doc.Insert(listId, 1, NewStr("second")); err != nil {
		t.Fatal(err)
	}
	if err := doc.Insert(listId, 2, NewStr("third")); err != nil {
		t.Fatal(err)
	}

	doc.Commit("nested", 2000)

	data, err := doc.Save()
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	doc2, err := Load(data)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Check nested map
	configVal, _, err := doc2.Get(Root, MapProp("config"))
	if err != nil {
		t.Fatalf("Get config: %v", err)
	}
	if !configVal.IsObject || configVal.ObjType != ObjTypeMap {
		t.Fatalf("config: expected map, got %s", configVal)
	}

	// We need the ObjId to read nested values - find it via the opset
	configId := findObjId(doc2, Root, "config")
	debugVal, _, err := doc2.Get(configId, MapProp("debug"))
	if err != nil {
		t.Fatalf("Get config.debug: %v", err)
	}
	if debugVal.Scalar.Type() != ScalarTypeTrue {
		t.Errorf("config.debug: expected true, got %s", debugVal)
	}

	portVal, _, err := doc2.Get(configId, MapProp("port"))
	if err != nil {
		t.Fatalf("Get config.port: %v", err)
	}
	if portVal.Scalar.Int() != 8080 {
		t.Errorf("config.port: expected 8080, got %d", portVal.Scalar.Int())
	}

	// Check list
	itemsVal, _, err := doc2.Get(Root, MapProp("items"))
	if err != nil {
		t.Fatalf("Get items: %v", err)
	}
	if !itemsVal.IsObject || itemsVal.ObjType != ObjTypeList {
		t.Fatalf("items: expected list, got %s", itemsVal)
	}
	itemsId := findObjId(doc2, Root, "items")
	if doc2.Length(itemsId) != 3 {
		t.Errorf("items: expected length 3, got %d", doc2.Length(itemsId))
	}
}

func TestSaveLoadText(t *testing.T) {
	doc := New()

	textId, err := doc.PutObject(Root, "notes", ObjTypeText)
	if err != nil {
		t.Fatal(err)
	}

	if err := doc.SpliceText(textId, 0, 0, "Hello World"); err != nil {
		t.Fatal(err)
	}
	doc.Commit("add text", 3000)

	data, err := doc.Save()
	if err != nil {
		t.Fatalf("Save: %v", err)
	}

	doc2, err := Load(data)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	notesId := findObjId(doc2, Root, "notes")
	text, err := doc2.Text(notesId)
	if err != nil {
		t.Fatalf("Text: %v", err)
	}
	if text != "Hello World" {
		t.Errorf("text: expected 'Hello World', got %q", text)
	}
}

func TestDoubleRoundTrip(t *testing.T) {
	// Create -> Save -> Load -> Save -> Load
	doc := New()
	doc.Put(Root, "x", NewInt(1))
	doc.Commit("v1", 1000)

	data1, _ := doc.Save()
	doc2, err := Load(data1)
	if err != nil {
		t.Fatalf("Load 1: %v", err)
	}

	data2, _ := doc2.Save()
	doc3, err := Load(data2)
	if err != nil {
		t.Fatalf("Load 2: %v", err)
	}

	val, _, err := doc3.Get(Root, MapProp("x"))
	if err != nil {
		t.Fatalf("Get x: %v", err)
	}
	if val.Scalar.Int() != 1 {
		t.Errorf("expected 1, got %d", val.Scalar.Int())
	}
}

func TestFixtureRoundTrip(t *testing.T) {
	// Load each fixture and save it back, then verify it loads again
	dir := fixturesDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Skipf("fixtures dir not found: %v", err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".automerge") {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			doc, err := Load(data)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}

			saved, err := doc.Save()
			if err != nil {
				t.Fatalf("Save: %v", err)
			}

			// Verify it starts with magic bytes
			if !bytes.HasPrefix(saved, []byte{0x85, 0x6f, 0x4a, 0x83}) {
				t.Fatalf("saved missing magic bytes")
			}

			doc2, err := Load(saved)
			if err != nil {
				t.Fatalf("Load round-trip: %v", err)
			}

			// Verify same number of ops
			if doc.opSet.Len() != doc2.opSet.Len() {
				t.Errorf("ops mismatch: %d vs %d", doc.opSet.Len(), doc2.opSet.Len())
			}
		})
	}
}

func TestFork(t *testing.T) {
	doc := New()
	doc.Put(Root, "x", NewInt(1))
	doc.Commit("v1", 1000)

	forked := doc.Fork()

	// Both docs should see x=1
	val, _, _ := forked.Get(Root, MapProp("x"))
	if val.Scalar.Int() != 1 {
		t.Errorf("fork: expected x=1, got %s", val)
	}

	// Mutating forked should not affect original
	if err := forked.Put(Root, "y", NewInt(2)); err != nil {
		t.Fatal(err)
	}
	_, _, err := doc.Get(Root, MapProp("y"))
	if err != ErrNotFound {
		t.Errorf("original should not have y after fork mutation")
	}

	// Forked should have different actor
	if doc.Actors()[0].Hex() == forked.Actors()[len(forked.Actors())-1].Hex() {
		t.Errorf("fork should have a different actor")
	}
}

func TestMerge(t *testing.T) {
	doc1 := New()
	if err := doc1.Put(Root, "x", NewInt(1)); err != nil {
		t.Fatal(err)
	}
	doc1.Commit("v1", 1000)

	doc2 := doc1.Fork()

	// Diverge
	if err := doc1.Put(Root, "a", NewStr("from doc1")); err != nil {
		t.Fatal(err)
	}
	doc1.Commit("doc1 change", 2000)

	if err := doc2.Put(Root, "b", NewStr("from doc2")); err != nil {
		t.Fatal(err)
	}
	doc2.Commit("doc2 change", 2000)

	// Merge doc2 into doc1
	if err := doc1.Merge(doc2); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// doc1 should have all keys
	keys := doc1.Keys(Root)
	if len(keys) != 3 {
		t.Errorf("expected 3 keys after merge, got %v", keys)
	}
	val, _, _ := doc1.Get(Root, MapProp("a"))
	if val.Scalar.Str() != "from doc1" {
		t.Errorf("a: expected 'from doc1', got %s", val)
	}
	val, _, _ = doc1.Get(Root, MapProp("b"))
	if val.Scalar.Str() != "from doc2" {
		t.Errorf("b: expected 'from doc2', got %s", val)
	}
}

func TestMergeConflict(t *testing.T) {
	doc1 := New()
	if err := doc1.Put(Root, "x", NewInt(1)); err != nil {
		t.Fatal(err)
	}
	doc1.Commit("v1", 1000)

	doc2 := doc1.Fork()

	// Both modify the same key
	if err := doc1.Put(Root, "x", NewInt(2)); err != nil {
		t.Fatal(err)
	}
	doc1.Commit("doc1", 2000)

	if err := doc2.Put(Root, "x", NewInt(3)); err != nil {
		t.Fatal(err)
	}
	doc2.Commit("doc2", 2000)

	if err := doc1.Merge(doc2); err != nil {
		t.Fatalf("Merge: %v", err)
	}

	// Should have a winner (deterministic by OpId) and a conflict
	allVals, _ := doc1.GetAll(Root, MapProp("x"))
	if len(allVals) != 2 {
		t.Errorf("expected 2 conflicting values, got %d", len(allVals))
	}

	// Winner should be accessible via Get
	val, _, _ := doc1.Get(Root, MapProp("x"))
	if val.Scalar.Int() != 2 && val.Scalar.Int() != 3 {
		t.Errorf("winner should be 2 or 3, got %d", val.Scalar.Int())
	}
}

func TestMergeRoundTrip(t *testing.T) {
	doc1 := New()
	if err := doc1.Put(Root, "x", NewInt(1)); err != nil {
		t.Fatal(err)
	}
	doc1.Commit("v1", 1000)

	doc2 := doc1.Fork()
	if err := doc1.Put(Root, "a", NewStr("hello")); err != nil {
		t.Fatal(err)
	}
	doc1.Commit("c1", 2000)
	if err := doc2.Put(Root, "b", NewStr("world")); err != nil {
		t.Fatal(err)
	}
	doc2.Commit("c2", 2000)
	if err := doc1.Merge(doc2); err != nil {
		t.Fatal(err)
	}

	// Save and reload
	data, _ := doc1.Save()
	doc3, err := Load(data)
	if err != nil {
		t.Fatalf("Load after merge: %v", err)
	}

	keys := doc3.Keys(Root)
	if len(keys) != 3 {
		t.Errorf("expected 3 keys after merge round-trip, got %v", keys)
	}
}

func TestMapRange(t *testing.T) {
	doc := New()
	doc.Put(Root, "a", NewInt(1))
	doc.Put(Root, "b", NewInt(2))
	doc.Put(Root, "c", NewInt(3))

	got := make(map[string]int64)
	for k, v := range doc.MapRange(Root) {
		got[k] = v.Scalar.Int()
	}

	if len(got) != 3 {
		t.Errorf("expected 3 entries, got %d", len(got))
	}
	if got["a"] != 1 || got["b"] != 2 || got["c"] != 3 {
		t.Errorf("unexpected values: %v", got)
	}
}

func TestListItems(t *testing.T) {
	doc := New()
	listId, _ := doc.PutObject(Root, "items", ObjTypeList)
	doc.Insert(listId, 0, NewStr("a"))
	doc.Insert(listId, 1, NewStr("b"))
	doc.Insert(listId, 2, NewStr("c"))

	var items []string
	for _, v := range doc.ListItems(listId) {
		items = append(items, v.Scalar.Str())
	}

	if len(items) != 3 || items[0] != "a" || items[1] != "b" || items[2] != "c" {
		t.Errorf("expected [a b c], got %v", items)
	}
}

func TestSplice(t *testing.T) {
	doc := New()
	listId, _ := doc.PutObject(Root, "items", ObjTypeList)
	doc.Insert(listId, 0, NewStr("a"))
	doc.Insert(listId, 1, NewStr("b"))
	doc.Insert(listId, 2, NewStr("c"))

	// Replace "b" with "x", "y"
	if err := doc.Splice(listId, 1, 1, NewStr("x"), NewStr("y")); err != nil {
		t.Fatal(err)
	}

	var items []string
	for _, v := range doc.ListItems(listId) {
		items = append(items, v.Scalar.Str())
	}

	if len(items) != 4 || items[0] != "a" || items[1] != "x" || items[2] != "y" || items[3] != "c" {
		t.Errorf("expected [a x y c], got %v", items)
	}
}

func TestParents(t *testing.T) {
	doc := New()
	mapId, _ := doc.PutObject(Root, "config", ObjTypeMap)
	doc.Put(mapId, "debug", NewBool(true))

	innerMapId, _ := doc.PutObject(mapId, "nested", ObjTypeMap)
	doc.Put(innerMapId, "deep", NewInt(42))

	path, err := doc.Parents(innerMapId)
	if err != nil {
		t.Fatalf("Parents: %v", err)
	}

	if len(path) != 2 {
		t.Fatalf("expected path length 2, got %d", len(path))
	}

	// First element is immediate parent (config map)
	if path[0].Prop.MapKey != "nested" {
		t.Errorf("expected prop 'nested', got %q", path[0].Prop.MapKey)
	}

	// Second element is root
	if !path[1].ObjId.IsRoot() {
		t.Errorf("expected root at end of path")
	}
	if path[1].Prop.MapKey != "config" {
		t.Errorf("expected prop 'config', got %q", path[1].Prop.MapKey)
	}
}

func TestParentsRoot(t *testing.T) {
	doc := New()
	path, err := doc.Parents(Root)
	if err != nil {
		t.Fatal(err)
	}
	if len(path) != 0 {
		t.Errorf("root should have empty path, got %d elements", len(path))
	}
}

// findObjId looks up the ObjId for a map key that contains an object.
func findObjId(d *Doc, obj ObjId, key string) ObjId {
	ops := d.opSet.OpsForObj(obj)
	for i := range ops {
		op := &ops[i]
		if op.Key.Kind == 0 && op.Key.MapKey == key && op.IsVisible() && op.Action.IsMake() {
			return ObjId{OpId: op.ID}
		}
	}
	return Root
}
