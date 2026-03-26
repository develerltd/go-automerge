package automerge

import (
	"testing"

	"github.com/develerltd/go-automerge/internal/types"
)

// exIdToObjId converts an ExId to an ObjId for use in tests.
func exIdToObjId(id ExId) ObjId {
	if id.IsRoot {
		return Root
	}
	return ObjId{OpId: OpId{Counter: id.Counter, ActorIdx: id.ActorIdx}}
}

func TestBatchCreateObjectFlatMap(t *testing.T) {
	doc := New()
	value := NewHydrateMap(map[string]HydrateValue{
		"a": NewHydrateScalar(types.NewStr("hello")),
		"b": NewHydrateScalar(types.NewInt(42)),
		"c": NewHydrateScalar(types.NewBool(true)),
	})

	objId, err := doc.BatchCreateObject(Root, MapProp("data"), value, false)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the object was created
	v, _, err := doc.Get(Root, MapProp("data"))
	if err != nil {
		t.Fatal(err)
	}
	if !v.IsObject || v.ObjType != ObjTypeMap {
		t.Fatalf("expected map object, got %v", v)
	}

	// Verify the map entries
	va, _, err := doc.Get(objId, MapProp("a"))
	if err != nil {
		t.Fatal(err)
	}
	if va.Scalar.Str() != "hello" {
		t.Fatalf("expected 'hello', got %v", va)
	}

	vb, _, err := doc.Get(objId, MapProp("b"))
	if err != nil {
		t.Fatal(err)
	}
	if vb.Scalar.Int() != 42 {
		t.Fatalf("expected 42, got %v", vb)
	}

	vc, _, err := doc.Get(objId, MapProp("c"))
	if err != nil {
		t.Fatal(err)
	}
	if !vc.Scalar.Bool() {
		t.Fatalf("expected true, got %v", vc)
	}
}

func TestBatchCreateObjectNestedMaps(t *testing.T) {
	doc := New()
	value := NewHydrateMap(map[string]HydrateValue{
		"outer": NewHydrateMap(map[string]HydrateValue{
			"inner_a": NewHydrateScalar(types.NewStr("deep")),
			"inner_b": NewHydrateScalar(types.NewInt(99)),
		}),
		"top_level": NewHydrateScalar(types.NewStr("flat")),
	})

	rootObj, err := doc.BatchCreateObject(Root, MapProp("nested"), value, false)
	if err != nil {
		t.Fatal(err)
	}

	// Check outer is a map
	outerVal, outerId, err := doc.Get(rootObj, MapProp("outer"))
	if err != nil {
		t.Fatal(err)
	}
	if !outerVal.IsObject || outerVal.ObjType != ObjTypeMap {
		t.Fatalf("expected map, got %v", outerVal)
	}

	outerObj := exIdToObjId(outerId)
	innerA, _, err := doc.Get(outerObj, MapProp("inner_a"))
	if err != nil {
		t.Fatal(err)
	}
	if innerA.Scalar.Str() != "deep" {
		t.Fatalf("expected 'deep', got %v", innerA)
	}

	innerB, _, err := doc.Get(outerObj, MapProp("inner_b"))
	if err != nil {
		t.Fatal(err)
	}
	if innerB.Scalar.Int() != 99 {
		t.Fatalf("expected 99, got %v", innerB)
	}

	topLevel, _, err := doc.Get(rootObj, MapProp("top_level"))
	if err != nil {
		t.Fatal(err)
	}
	if topLevel.Scalar.Str() != "flat" {
		t.Fatalf("expected 'flat', got %v", topLevel)
	}
}

func TestBatchCreateObjectList(t *testing.T) {
	doc := New()
	value := NewHydrateList([]HydrateValue{
		NewHydrateScalar(types.NewInt(1)),
		NewHydrateList([]HydrateValue{
			NewHydrateScalar(types.NewInt(2)),
			NewHydrateScalar(types.NewInt(3)),
		}),
		NewHydrateMap(map[string]HydrateValue{
			"a": NewHydrateScalar(types.NewInt(4)),
		}),
	})

	listObj, err := doc.BatchCreateObject(Root, MapProp("data"), value, false)
	if err != nil {
		t.Fatal(err)
	}

	// Verify length
	length := doc.Length(listObj)
	if length != 3 {
		t.Fatalf("expected length 3, got %d", length)
	}

	// First element: scalar 1
	v0, _, err := doc.Get(listObj, SeqProp(0))
	if err != nil {
		t.Fatal(err)
	}
	if v0.Scalar.Int() != 1 {
		t.Fatalf("expected 1, got %v", v0)
	}

	// Second element: nested list [2, 3]
	v1, v1Id, err := doc.Get(listObj, SeqProp(1))
	if err != nil {
		t.Fatal(err)
	}
	if !v1.IsObject || v1.ObjType != ObjTypeList {
		t.Fatalf("expected list, got %v", v1)
	}
	innerList := exIdToObjId(v1Id)
	innerLen := doc.Length(innerList)
	if innerLen != 2 {
		t.Fatalf("expected inner length 2, got %d", innerLen)
	}

	// Third element: nested map {a: 4}
	v2, v2Id, err := doc.Get(listObj, SeqProp(2))
	if err != nil {
		t.Fatal(err)
	}
	if !v2.IsObject || v2.ObjType != ObjTypeMap {
		t.Fatalf("expected map, got %v", v2)
	}
	innerMap := exIdToObjId(v2Id)
	va, _, err := doc.Get(innerMap, MapProp("a"))
	if err != nil {
		t.Fatal(err)
	}
	if va.Scalar.Int() != 4 {
		t.Fatalf("expected 4, got %v", va)
	}
}

func TestBatchCreateObjectText(t *testing.T) {
	doc := New()
	value := NewHydrateText("hello world")

	textObj, err := doc.BatchCreateObject(Root, MapProp("content"), value, false)
	if err != nil {
		t.Fatal(err)
	}

	text, err := doc.Text(textObj)
	if err != nil {
		t.Fatal(err)
	}
	if text != "hello world" {
		t.Fatalf("expected 'hello world', got %q", text)
	}
}

func TestBatchCreateObjectInsertIntoList(t *testing.T) {
	doc := New()
	listObj, err := doc.PutObject(Root, "mylist", ObjTypeList)
	if err != nil {
		t.Fatal(err)
	}
	// Insert some initial items
	if err := doc.Insert(listObj, 0, types.NewStr("first")); err != nil {
		t.Fatal(err)
	}
	if err := doc.Insert(listObj, 1, types.NewStr("third")); err != nil {
		t.Fatal(err)
	}

	// Insert a nested map at index 1 (between "first" and "third")
	nestedValue := NewHydrateMap(map[string]HydrateValue{
		"x": NewHydrateScalar(types.NewInt(100)),
	})
	_, err = doc.BatchCreateObject(listObj, SeqProp(1), nestedValue, true)
	if err != nil {
		t.Fatal(err)
	}

	// Verify order: "first", {x: 100}, "third"
	length := doc.Length(listObj)
	if length != 3 {
		t.Fatalf("expected length 3, got %d", length)
	}

	v0, _, _ := doc.Get(listObj, SeqProp(0))
	if v0.Scalar.Str() != "first" {
		t.Fatalf("expected 'first', got %v", v0)
	}

	v1, _, _ := doc.Get(listObj, SeqProp(1))
	if !v1.IsObject || v1.ObjType != ObjTypeMap {
		t.Fatalf("expected map at index 1, got %v", v1)
	}

	v2, _, _ := doc.Get(listObj, SeqProp(2))
	if v2.Scalar.Str() != "third" {
		t.Fatalf("expected 'third', got %v", v2)
	}
}

func TestInitFromHydrate(t *testing.T) {
	doc := New()
	err := doc.InitFromHydrate(map[string]HydrateValue{
		"name":  NewHydrateScalar(types.NewStr("Alice")),
		"age":   NewHydrateScalar(types.NewInt(30)),
		"tags":  NewHydrateList([]HydrateValue{
			NewHydrateScalar(types.NewStr("admin")),
			NewHydrateScalar(types.NewStr("user")),
		}),
		"meta": NewHydrateMap(map[string]HydrateValue{
			"created": NewHydrateScalar(types.NewStr("2024-01-01")),
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	name, _, err := doc.Get(Root, MapProp("name"))
	if err != nil {
		t.Fatal(err)
	}
	if name.Scalar.Str() != "Alice" {
		t.Fatalf("expected 'Alice', got %v", name)
	}

	age, _, err := doc.Get(Root, MapProp("age"))
	if err != nil {
		t.Fatal(err)
	}
	if age.Scalar.Int() != 30 {
		t.Fatalf("expected 30, got %v", age)
	}

	tagsVal, tagsId, err := doc.Get(Root, MapProp("tags"))
	if err != nil {
		t.Fatal(err)
	}
	if !tagsVal.IsObject || tagsVal.ObjType != ObjTypeList {
		t.Fatalf("expected list, got %v", tagsVal)
	}
	tagsObj := exIdToObjId(tagsId)
	tagsLen := doc.Length(tagsObj)
	if tagsLen != 2 {
		t.Fatalf("expected 2 tags, got %d", tagsLen)
	}

	metaVal, metaId, err := doc.Get(Root, MapProp("meta"))
	if err != nil {
		t.Fatal(err)
	}
	if !metaVal.IsObject || metaVal.ObjType != ObjTypeMap {
		t.Fatalf("expected map, got %v", metaVal)
	}
	metaObj := exIdToObjId(metaId)
	created, _, err := doc.Get(metaObj, MapProp("created"))
	if err != nil {
		t.Fatal(err)
	}
	if created.Scalar.Str() != "2024-01-01" {
		t.Fatalf("expected '2024-01-01', got %v", created)
	}
}

func TestSpliceValuesWithNestedObjects(t *testing.T) {
	doc := New()
	listObj, err := doc.PutObject(Root, "list", ObjTypeList)
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Insert(listObj, 0, types.NewStr("a")); err != nil {
		t.Fatal(err)
	}
	if err := doc.Insert(listObj, 1, types.NewStr("b")); err != nil {
		t.Fatal(err)
	}

	// Splice: delete "b", insert a nested map and a scalar
	err = doc.SpliceValues(listObj, 1, 1,
		NewHydrateMap(map[string]HydrateValue{
			"x": NewHydrateScalar(types.NewInt(10)),
		}),
		NewHydrateScalar(types.NewStr("c")),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Result should be: "a", {x: 10}, "c"
	length := doc.Length(listObj)
	if length != 3 {
		t.Fatalf("expected length 3, got %d", length)
	}

	v0, _, _ := doc.Get(listObj, SeqProp(0))
	if v0.Scalar.Str() != "a" {
		t.Fatalf("expected 'a', got %v", v0)
	}

	v1, _, _ := doc.Get(listObj, SeqProp(1))
	if !v1.IsObject || v1.ObjType != ObjTypeMap {
		t.Fatalf("expected map, got %v", v1)
	}

	v2, _, _ := doc.Get(listObj, SeqProp(2))
	if v2.Scalar.Str() != "c" {
		t.Fatalf("expected 'c', got %v", v2)
	}
}

func TestBatchCreateObjectSaveLoad(t *testing.T) {
	doc := New()
	value := NewHydrateMap(map[string]HydrateValue{
		"name": NewHydrateScalar(types.NewStr("test")),
		"items": NewHydrateList([]HydrateValue{
			NewHydrateScalar(types.NewInt(1)),
			NewHydrateScalar(types.NewInt(2)),
		}),
		"content": NewHydrateText("hello"),
	})

	_, err := doc.BatchCreateObject(Root, MapProp("data"), value, false)
	if err != nil {
		t.Fatal(err)
	}
	doc.Commit("batch create", 0)

	// Save
	data, err := doc.Save()
	if err != nil {
		t.Fatal(err)
	}

	// Load
	doc2, err := Load(data)
	if err != nil {
		t.Fatal(err)
	}

	// Verify
	dataVal, dataId, err := doc2.Get(Root, MapProp("data"))
	if err != nil {
		t.Fatal(err)
	}
	if !dataVal.IsObject || dataVal.ObjType != ObjTypeMap {
		t.Fatalf("expected map, got %v", dataVal)
	}
	dataObj := exIdToObjId(dataId)

	name, _, err := doc2.Get(dataObj, MapProp("name"))
	if err != nil {
		t.Fatal(err)
	}
	if name.Scalar.Str() != "test" {
		t.Fatalf("expected 'test', got %v", name)
	}

	itemsVal, itemsId, err := doc2.Get(dataObj, MapProp("items"))
	if err != nil {
		t.Fatal(err)
	}
	if !itemsVal.IsObject || itemsVal.ObjType != ObjTypeList {
		t.Fatalf("expected list, got %v", itemsVal)
	}
	itemsObj := exIdToObjId(itemsId)
	if doc2.Length(itemsObj) != 2 {
		t.Fatalf("expected 2 items, got %d", doc2.Length(itemsObj))
	}

	contentVal, contentId, err := doc2.Get(dataObj, MapProp("content"))
	if err != nil {
		t.Fatal(err)
	}
	if !contentVal.IsObject || contentVal.ObjType != ObjTypeText {
		t.Fatalf("expected text, got %v", contentVal)
	}
	contentObj := exIdToObjId(contentId)
	text, err := doc2.Text(contentObj)
	if err != nil {
		t.Fatal(err)
	}
	if text != "hello" {
		t.Fatalf("expected 'hello', got %q", text)
	}
}

func TestBatchCreateObjectMerge(t *testing.T) {
	doc1 := New()
	doc2 := doc1.Fork()

	// Create a batch object on doc1
	value := NewHydrateMap(map[string]HydrateValue{
		"x": NewHydrateScalar(types.NewInt(1)),
	})
	_, err := doc1.BatchCreateObject(Root, MapProp("from_doc1"), value, false)
	if err != nil {
		t.Fatal(err)
	}
	doc1.Commit("doc1 batch", 0)

	// Create something on doc2
	if err := doc2.Put(Root, "from_doc2", types.NewStr("hello")); err != nil {
		t.Fatal(err)
	}
	doc2.Commit("doc2 change", 0)

	// Merge
	if err := doc1.Merge(doc2); err != nil {
		t.Fatal(err)
	}

	// Verify both changes are present
	v1, _, err := doc1.Get(Root, MapProp("from_doc1"))
	if err != nil {
		t.Fatal(err)
	}
	if !v1.IsObject || v1.ObjType != ObjTypeMap {
		t.Fatalf("expected map, got %v", v1)
	}

	v2, _, err := doc1.Get(Root, MapProp("from_doc2"))
	if err != nil {
		t.Fatal(err)
	}
	if v2.Scalar.Str() != "hello" {
		t.Fatalf("expected 'hello', got %v", v2)
	}
}

func BenchmarkBatchVsIndividual(b *testing.B) {
	// Build a moderately large nested structure
	makeValue := func() map[string]HydrateValue {
		items := make([]HydrateValue, 50)
		for i := range items {
			items[i] = NewHydrateMap(map[string]HydrateValue{
				"id":   NewHydrateScalar(types.NewInt(int64(i))),
				"name": NewHydrateScalar(types.NewStr("item")),
				"tags": NewHydrateList([]HydrateValue{
					NewHydrateScalar(types.NewStr("a")),
					NewHydrateScalar(types.NewStr("b")),
				}),
			})
		}
		return map[string]HydrateValue{
			"items": NewHydrateList(items),
		}
	}

	b.Run("Batch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			doc := New()
			_ = doc.InitFromHydrate(makeValue())
		}
	})

	b.Run("Individual", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			doc := New()
			listObj, _ := doc.PutObject(Root, "items", ObjTypeList)
			for j := 0; j < 50; j++ {
				mapObj, _ := doc.InsertObject(listObj, uint64(j), ObjTypeMap)
				_ = doc.Put(mapObj, "id", types.NewInt(int64(j)))
				_ = doc.Put(mapObj, "name", types.NewStr("item"))
				tagsObj, _ := doc.PutObject(mapObj, "tags", ObjTypeList)
				_ = doc.Insert(tagsObj, 0, types.NewStr("a"))
				_ = doc.Insert(tagsObj, 1, types.NewStr("b"))
			}
		}
	})
}
