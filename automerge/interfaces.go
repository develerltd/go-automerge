package automerge

import "iter"

// ReadDoc defines the read-only interface for an automerge document.
// Both Doc and AutoCommit implement this interface.
type ReadDoc interface {
	// Get returns the winning visible value at the given property.
	Get(obj ObjId, prop Prop) (Value, ExId, error)

	// GetAll returns all visible values (including conflicts) at the given property.
	GetAll(obj ObjId, prop Prop) ([]ValueWithId, error)

	// Keys returns the visible map keys for the given object, sorted.
	Keys(obj ObjId) []string

	// Length returns the number of visible elements in a list/text, or keys in a map.
	Length(obj ObjId) uint64

	// Text returns the text content of a text object.
	Text(obj ObjId) (string, error)

	// MapRange returns an iterator over visible key-value pairs of a map object.
	MapRange(obj ObjId) iter.Seq2[string, Value]

	// ListItems returns an iterator over visible index-value pairs of a list/text object.
	ListItems(obj ObjId) iter.Seq2[uint64, Value]

	// Heads returns the current heads of the document.
	Heads() []ChangeHash

	// Actors returns the actor table.
	Actors() []ActorId

	// Parents returns the path from the given object to the root.
	Parents(obj ObjId) ([]PathElement, error)

	// Marks returns all marks on the given text object.
	Marks(obj ObjId) ([]Mark, error)

	// GetCursor returns a cursor for the element at the given index.
	GetCursor(obj ObjId, index uint64, move MoveCursor) (Cursor, error)

	// GetCursorPosition resolves a cursor to a position in the sequence.
	GetCursorPosition(obj ObjId, cursor Cursor) (CursorPosition, error)

	// Historical queries
	GetAt(obj ObjId, prop Prop, heads []ChangeHash) (Value, ExId, error)
	GetAllAt(obj ObjId, prop Prop, heads []ChangeHash) ([]ValueWithId, error)
	KeysAt(obj ObjId, heads []ChangeHash) []string
	LengthAt(obj ObjId, heads []ChangeHash) uint64
	TextAt(obj ObjId, heads []ChangeHash) (string, error)
	MapRangeAt(obj ObjId, heads []ChangeHash) iter.Seq2[string, Value]
	ListItemsAt(obj ObjId, heads []ChangeHash) iter.Seq2[uint64, Value]
	MarksAt(obj ObjId, heads []ChangeHash) ([]Mark, error)
}

// Transactable defines the write interface for an automerge document.
// Both Doc and AutoCommit implement this interface.
type Transactable interface {
	// Put sets a scalar value at a map key.
	Put(obj ObjId, key string, value ScalarValue) error

	// PutObject creates a new object at a map key and returns its ID.
	PutObject(obj ObjId, key string, objType ObjType) (ObjId, error)

	// Delete removes a value at a map key or sequence index.
	Delete(obj ObjId, prop Prop) error

	// Insert inserts a scalar value at the given index of a list/text object.
	Insert(obj ObjId, index uint64, value ScalarValue) error

	// InsertObject inserts a new object at the given index of a list.
	InsertObject(obj ObjId, index uint64, objType ObjType) (ObjId, error)

	// Increment increments a counter value at a map key.
	Increment(obj ObjId, key string, by int64) error

	// SpliceText replaces del characters at pos with text in a text object.
	SpliceText(obj ObjId, pos, del uint64, text string) error

	// Splice replaces del elements at pos in a list with the given values.
	Splice(obj ObjId, pos, del uint64, vals ...ScalarValue) error

	// Mark creates a mark on a range of a text object.
	Mark(obj ObjId, start, end uint64, expand ExpandMark, name string, value ScalarValue) error

	// BatchCreateObject creates a nested object tree at the given property
	// using batch insertion for efficiency. The value must be a map, list,
	// or text HydrateValue.
	BatchCreateObject(obj ObjId, prop Prop, value HydrateValue, insert bool) (ObjId, error)

	// InitFromHydrate initializes the root map from a map of HydrateValues.
	// Existing keys not in the map are left unchanged.
	InitFromHydrate(value map[string]HydrateValue) error

	// SpliceValues replaces del elements at pos with the given values,
	// which can include nested objects (maps, lists, text).
	SpliceValues(obj ObjId, pos, del uint64, vals ...HydrateValue) error
}

// Compile-time interface checks.
var (
	_ ReadDoc      = (*Doc)(nil)
	_ Transactable = (*Doc)(nil)
)
