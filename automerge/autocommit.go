package automerge

import (
	"iter"
	"time"
)

// AutoCommit wraps a Doc and automatically commits after each mutation.
// This is convenient for simple use cases where explicit transaction management
// is not needed.
type AutoCommit struct {
	doc     *Doc
	message string
}

// NewAutoCommit creates a new empty auto-committing document with a random actor ID.
func NewAutoCommit() *AutoCommit {
	return &AutoCommit{doc: New()}
}

// NewAutoCommitWithActorId creates a new empty auto-committing document with the given actor ID.
func NewAutoCommitWithActorId(actor ActorId) *AutoCommit {
	return &AutoCommit{doc: NewWithActorId(actor)}
}

// LoadAutoCommit loads an auto-committing document from binary data.
func LoadAutoCommit(data []byte) (*AutoCommit, error) {
	doc, err := Load(data)
	if err != nil {
		return nil, err
	}
	return &AutoCommit{doc: doc}, nil
}

// SetMessage sets the commit message for the next auto-commit.
func (ac *AutoCommit) SetMessage(msg string) {
	ac.message = msg
}

// Doc returns the underlying Doc.
func (ac *AutoCommit) Doc() *Doc {
	return ac.doc
}

func (ac *AutoCommit) autoCommit() {
	if len(ac.doc.pendingOps) > 0 {
		ac.doc.Commit(ac.message, time.Now().UnixMilli())
		ac.message = ""
	}
}

// --- Transactable (write) methods ---

func (ac *AutoCommit) Put(obj ObjId, key string, value ScalarValue) error {
	err := ac.doc.Put(obj, key, value)
	if err == nil {
		ac.autoCommit()
	}
	return err
}

func (ac *AutoCommit) PutObject(obj ObjId, key string, objType ObjType) (ObjId, error) {
	id, err := ac.doc.PutObject(obj, key, objType)
	if err == nil {
		ac.autoCommit()
	}
	return id, err
}

func (ac *AutoCommit) Delete(obj ObjId, prop Prop) error {
	err := ac.doc.Delete(obj, prop)
	if err == nil {
		ac.autoCommit()
	}
	return err
}

func (ac *AutoCommit) Insert(obj ObjId, index uint64, value ScalarValue) error {
	err := ac.doc.Insert(obj, index, value)
	if err == nil {
		ac.autoCommit()
	}
	return err
}

func (ac *AutoCommit) InsertObject(obj ObjId, index uint64, objType ObjType) (ObjId, error) {
	id, err := ac.doc.InsertObject(obj, index, objType)
	if err == nil {
		ac.autoCommit()
	}
	return id, err
}

func (ac *AutoCommit) Increment(obj ObjId, key string, by int64) error {
	err := ac.doc.Increment(obj, key, by)
	if err == nil {
		ac.autoCommit()
	}
	return err
}

func (ac *AutoCommit) SpliceText(obj ObjId, pos, del uint64, text string) error {
	err := ac.doc.SpliceText(obj, pos, del, text)
	if err == nil {
		ac.autoCommit()
	}
	return err
}

func (ac *AutoCommit) Splice(obj ObjId, pos, del uint64, vals ...ScalarValue) error {
	err := ac.doc.Splice(obj, pos, del, vals...)
	if err == nil {
		ac.autoCommit()
	}
	return err
}

func (ac *AutoCommit) Mark(obj ObjId, start, end uint64, expand ExpandMark, name string, value ScalarValue) error {
	err := ac.doc.Mark(obj, start, end, expand, name, value)
	if err == nil {
		ac.autoCommit()
	}
	return err
}

func (ac *AutoCommit) BatchCreateObject(obj ObjId, prop Prop, value HydrateValue, insert bool) (ObjId, error) {
	id, err := ac.doc.BatchCreateObject(obj, prop, value, insert)
	if err == nil {
		ac.autoCommit()
	}
	return id, err
}

func (ac *AutoCommit) InitFromHydrate(value map[string]HydrateValue) error {
	err := ac.doc.InitFromHydrate(value)
	if err == nil {
		ac.autoCommit()
	}
	return err
}

func (ac *AutoCommit) SpliceValues(obj ObjId, pos, del uint64, vals ...HydrateValue) error {
	err := ac.doc.SpliceValues(obj, pos, del, vals...)
	if err == nil {
		ac.autoCommit()
	}
	return err
}

// --- ReadDoc (read) methods ---

func (ac *AutoCommit) Get(obj ObjId, prop Prop) (Value, ExId, error) {
	return ac.doc.Get(obj, prop)
}

func (ac *AutoCommit) GetAll(obj ObjId, prop Prop) ([]ValueWithId, error) {
	return ac.doc.GetAll(obj, prop)
}

func (ac *AutoCommit) Keys(obj ObjId) []string {
	return ac.doc.Keys(obj)
}

func (ac *AutoCommit) Length(obj ObjId) uint64 {
	return ac.doc.Length(obj)
}

func (ac *AutoCommit) Text(obj ObjId) (string, error) {
	return ac.doc.Text(obj)
}

func (ac *AutoCommit) MapRange(obj ObjId) iter.Seq2[string, Value] {
	return ac.doc.MapRange(obj)
}

func (ac *AutoCommit) ListItems(obj ObjId) iter.Seq2[uint64, Value] {
	return ac.doc.ListItems(obj)
}

func (ac *AutoCommit) Heads() []ChangeHash {
	return ac.doc.Heads()
}

func (ac *AutoCommit) Actors() []ActorId {
	return ac.doc.Actors()
}

func (ac *AutoCommit) Parents(obj ObjId) ([]PathElement, error) {
	return ac.doc.Parents(obj)
}

func (ac *AutoCommit) Marks(obj ObjId) ([]Mark, error) {
	return ac.doc.Marks(obj)
}

func (ac *AutoCommit) GetCursor(obj ObjId, index uint64, move MoveCursor) (Cursor, error) {
	return ac.doc.GetCursor(obj, index, move)
}

func (ac *AutoCommit) GetCursorPosition(obj ObjId, cursor Cursor) (CursorPosition, error) {
	return ac.doc.GetCursorPosition(obj, cursor)
}

func (ac *AutoCommit) GetAt(obj ObjId, prop Prop, heads []ChangeHash) (Value, ExId, error) {
	return ac.doc.GetAt(obj, prop, heads)
}

func (ac *AutoCommit) GetAllAt(obj ObjId, prop Prop, heads []ChangeHash) ([]ValueWithId, error) {
	return ac.doc.GetAllAt(obj, prop, heads)
}

func (ac *AutoCommit) KeysAt(obj ObjId, heads []ChangeHash) []string {
	return ac.doc.KeysAt(obj, heads)
}

func (ac *AutoCommit) LengthAt(obj ObjId, heads []ChangeHash) uint64 {
	return ac.doc.LengthAt(obj, heads)
}

func (ac *AutoCommit) TextAt(obj ObjId, heads []ChangeHash) (string, error) {
	return ac.doc.TextAt(obj, heads)
}

func (ac *AutoCommit) MapRangeAt(obj ObjId, heads []ChangeHash) iter.Seq2[string, Value] {
	return ac.doc.MapRangeAt(obj, heads)
}

func (ac *AutoCommit) ListItemsAt(obj ObjId, heads []ChangeHash) iter.Seq2[uint64, Value] {
	return ac.doc.ListItemsAt(obj, heads)
}

func (ac *AutoCommit) MarksAt(obj ObjId, heads []ChangeHash) ([]Mark, error) {
	return ac.doc.MarksAt(obj, heads)
}

// Save serializes the document to the automerge binary format.
func (ac *AutoCommit) Save() ([]byte, error) {
	return ac.doc.Save()
}

// Fork creates a copy with a new random actor ID.
func (ac *AutoCommit) Fork() *AutoCommit {
	return &AutoCommit{doc: ac.doc.Fork()}
}

// Merge incorporates all changes from other into this document.
func (ac *AutoCommit) Merge(other *AutoCommit) error {
	return ac.doc.Merge(other.doc)
}

// Diff computes patches between two document states.
func (ac *AutoCommit) Diff(oldHeads, newHeads []ChangeHash) []Patch {
	return ac.doc.Diff(oldHeads, newHeads)
}

// Compile-time interface checks.
var (
	_ ReadDoc      = (*AutoCommit)(nil)
	_ Transactable = (*AutoCommit)(nil)
)
