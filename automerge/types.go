package automerge

import t "github.com/develerltd/go-automerge/internal/types"

// Re-export core types from internal/types.
type (
	ActorId      = t.ActorId
	OpId         = t.OpId
	ObjId        = t.ObjId
	ElemId       = t.ElemId
	ChangeHash   = t.ChangeHash
	ExId         = t.ExId
	ObjType      = t.ObjType
	PropKind     = t.PropKind
	Prop         = t.Prop
	ScalarType   = t.ScalarType
	ScalarValue  = t.ScalarValue
	Value        = t.Value
	HydrateKind  = t.HydrateKind
	HydrateValue = t.HydrateValue
)

// Re-export constants and constructors.
var (
	Root    = t.Root
	Head    = t.Head
	RootExId = t.RootExId
	MapProp  = t.MapProp
	SeqProp  = t.SeqProp
	NewActorId = t.NewActorId
)

const (
	ObjTypeMap   = t.ObjTypeMap
	ObjTypeList  = t.ObjTypeList
	ObjTypeText  = t.ObjTypeText
	ObjTypeTable = t.ObjTypeTable
	PropKindMap  = t.PropKindMap
	PropKindSeq  = t.PropKindSeq
	ScalarTypeNull      = t.ScalarTypeNull
	ScalarTypeFalse     = t.ScalarTypeFalse
	ScalarTypeTrue      = t.ScalarTypeTrue
	ScalarTypeUint      = t.ScalarTypeUint
	ScalarTypeInt       = t.ScalarTypeInt
	ScalarTypeFloat64   = t.ScalarTypeFloat64
	ScalarTypeString    = t.ScalarTypeString
	ScalarTypeBytes     = t.ScalarTypeBytes
	ScalarTypeCounter   = t.ScalarTypeCounter
	ScalarTypeTimestamp  = t.ScalarTypeTimestamp
	HydrateScalar = t.HydrateScalar
	HydrateMap    = t.HydrateMap
	HydrateList   = t.HydrateList
	HydrateText   = t.HydrateText
)

// Re-export value constructors.
var (
	NewNull          = t.NewNull
	NewBool          = t.NewBool
	NewInt           = t.NewInt
	NewUint          = t.NewUint
	NewFloat64       = t.NewFloat64
	NewStr           = t.NewStr
	NewBytes         = t.NewBytes
	NewCounter       = t.NewCounter
	NewTimestamp      = t.NewTimestamp
	NewUnknownScalar = t.NewUnknownScalar
	NewObjectValue    = t.NewObjectValue
	NewScalarValue    = t.NewScalarValue
	NewHydrateScalar  = t.NewHydrateScalar
	NewHydrateMap     = t.NewHydrateMap
	NewHydrateList    = t.NewHydrateList
	NewHydrateText    = t.NewHydrateText
)
