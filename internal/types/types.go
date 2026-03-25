package types

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// ActorId is a unique identifier for an actor (typically 16 random bytes).
type ActorId []byte

// NewActorId generates a random 16-byte actor ID.
func NewActorId() ActorId {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		panic("failed to generate random actor ID: " + err.Error())
	}
	return ActorId(id)
}

func (a ActorId) Hex() string   { return hex.EncodeToString(a) }
func (a ActorId) Bytes() []byte { return []byte(a) }
func (a ActorId) String() string { return a.Hex() }
func (a ActorId) Compare(other ActorId) int { return bytes.Compare(a, other) }

// OpId is an operation identifier: (counter, actor_index).
type OpId struct {
	Counter  uint64
	ActorIdx uint32
}

func (o OpId) Compare(other OpId) int {
	if o.Counter != other.Counter {
		if o.Counter < other.Counter {
			return -1
		}
		return 1
	}
	if o.ActorIdx != other.ActorIdx {
		if o.ActorIdx < other.ActorIdx {
			return -1
		}
		return 1
	}
	return 0
}

func (o OpId) String() string { return fmt.Sprintf("%d@%d", o.Counter, o.ActorIdx) }
func (o OpId) IsZero() bool   { return o.Counter == 0 && o.ActorIdx == 0 }

// ObjId identifies an object in the document.
type ObjId struct{ OpId }

var Root = ObjId{}

func (o ObjId) IsRoot() bool { return o.Counter == 0 }

// ElemId identifies an element in a sequence.
type ElemId struct{ OpId }

var Head = ElemId{}

// ChangeHash is a SHA-256 hash identifying a specific change.
type ChangeHash [32]byte

func (h ChangeHash) String() string { return hex.EncodeToString(h[:]) }

// ExId is the external (stable) representation of an object ID.
type ExId struct {
	IsRoot   bool
	Counter  uint64
	Actor    ActorId
	ActorIdx uint32
}

func RootExId() ExId { return ExId{IsRoot: true} }

func (e ExId) String() string {
	if e.IsRoot {
		return "_root"
	}
	return fmt.Sprintf("%d@%s", e.Counter, e.Actor.Hex())
}

// ObjType represents the type of a composite object.
type ObjType uint8

const (
	ObjTypeMap   ObjType = 0
	ObjTypeList  ObjType = 2
	ObjTypeText  ObjType = 4
	ObjTypeTable ObjType = 6
)

func (ot ObjType) String() string {
	switch ot {
	case ObjTypeMap:
		return "map"
	case ObjTypeList:
		return "list"
	case ObjTypeText:
		return "text"
	case ObjTypeTable:
		return "table"
	default:
		return fmt.Sprintf("unknown(%d)", ot)
	}
}

func (ot ObjType) IsSequence() bool { return ot == ObjTypeList || ot == ObjTypeText }

// PropKind distinguishes map keys from sequence indices.
type PropKind int

const (
	PropKindMap PropKind = iota
	PropKindSeq
)

// Prop represents a property: either a map key (string) or a sequence index.
type Prop struct {
	Kind     PropKind
	MapKey   string
	SeqIndex uint64
}

func MapProp(key string) Prop        { return Prop{Kind: PropKindMap, MapKey: key} }
func SeqProp(index uint64) Prop      { return Prop{Kind: PropKindSeq, SeqIndex: index} }
func (p Prop) String() string {
	if p.Kind == PropKindMap {
		return p.MapKey
	}
	return fmt.Sprintf("%d", p.SeqIndex)
}
