package types

// HydrateKind describes the kind of a HydrateValue.
type HydrateKind uint8

const (
	HydrateScalar HydrateKind = iota
	HydrateMap
	HydrateList
	HydrateText
)

// HydrateValue represents a recursive document value that can be a scalar,
// map, list, or text. It is used for batch insertion of nested objects.
type HydrateValue struct {
	Kind       HydrateKind
	Scalar     ScalarValue
	MapEntries map[string]HydrateValue
	ListItems  []HydrateValue
	Text       string
}

// NewHydrateScalar creates a HydrateValue wrapping a scalar.
func NewHydrateScalar(v ScalarValue) HydrateValue {
	return HydrateValue{Kind: HydrateScalar, Scalar: v}
}

// NewHydrateMap creates a HydrateValue wrapping a map of string keys to values.
func NewHydrateMap(entries map[string]HydrateValue) HydrateValue {
	return HydrateValue{Kind: HydrateMap, MapEntries: entries}
}

// NewHydrateList creates a HydrateValue wrapping a list of values.
func NewHydrateList(items []HydrateValue) HydrateValue {
	return HydrateValue{Kind: HydrateList, ListItems: items}
}

// NewHydrateText creates a HydrateValue wrapping a text string.
func NewHydrateText(text string) HydrateValue {
	return HydrateValue{Kind: HydrateText, Text: text}
}

// ObjType returns the ObjType for this hydrate value, or false if it's a scalar.
func (v HydrateValue) ObjType() (ObjType, bool) {
	switch v.Kind {
	case HydrateMap:
		return ObjTypeMap, true
	case HydrateList:
		return ObjTypeList, true
	case HydrateText:
		return ObjTypeText, true
	default:
		return 0, false
	}
}
