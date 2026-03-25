package automerge

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/develerltd/go-automerge/internal/encoding"
	"github.com/develerltd/go-automerge/internal/opset"
	"github.com/develerltd/go-automerge/internal/types"
)

// MoveCursor determines how a cursor resolves when its referenced item is removed.
type MoveCursor int

const (
	// MoveBefore shifts to the previous visible item (or position 0).
	MoveBefore MoveCursor = iota
	// MoveAfter shifts to the next visible item (or sequence length).
	MoveAfter
)

// CursorKind distinguishes the three cursor variants.
type CursorKind int

const (
	CursorStart CursorKind = iota
	CursorEnd
	CursorOp
)

// Cursor identifies a stable position in a sequence (list or text).
type Cursor struct {
	Kind       CursorKind
	Counter    uint64
	Actor      ActorId
	MoveCursor MoveCursor
}

// StartCursor returns a cursor that always resolves to position 0.
func StartCursor() Cursor { return Cursor{Kind: CursorStart} }

// EndCursor returns a cursor that always resolves to sequence length.
func EndCursor() Cursor { return Cursor{Kind: CursorEnd} }

// Wire format tags
const (
	cursorVersionTag  = 1
	cursorStartTag    = 1
	cursorEndTag      = 2
	cursorOpTag       = 3
	cursorMoveBeforeTag = 1
	cursorMoveAfterTag  = 2
)

// ToBytes serializes the cursor to its binary representation.
func (c Cursor) ToBytes() []byte {
	switch c.Kind {
	case CursorStart:
		return []byte{cursorVersionTag, cursorStartTag}
	case CursorEnd:
		return []byte{cursorVersionTag, cursorEndTag}
	case CursorOp:
		actorBytes := []byte(c.Actor)
		buf := make([]byte, 0, 2+10+len(actorBytes)+10+1)
		buf = append(buf, cursorVersionTag, cursorOpTag)
		buf = encoding.AppendULEB128(buf, uint64(len(actorBytes)))
		buf = append(buf, actorBytes...)
		buf = encoding.AppendULEB128(buf, c.Counter)
		if c.MoveCursor == MoveBefore {
			buf = append(buf, cursorMoveBeforeTag)
		} else {
			buf = append(buf, cursorMoveAfterTag)
		}
		return buf
	default:
		return nil
	}
}

// CursorFromBytes parses a cursor from its binary representation.
func CursorFromBytes(data []byte) (Cursor, error) {
	if len(data) < 2 {
		return Cursor{}, fmt.Errorf("cursor data too short")
	}

	version := data[0]
	if version == 0 {
		return parseCursorV0(data[1:])
	}
	if version != cursorVersionTag {
		return Cursor{}, fmt.Errorf("unknown cursor version %d", version)
	}

	tag := data[1]
	switch tag {
	case cursorStartTag:
		return StartCursor(), nil
	case cursorEndTag:
		return EndCursor(), nil
	case cursorOpTag:
		offset := 2
		actorLen, offset, err := encoding.ReadULEB128(data, offset)
		if err != nil {
			return Cursor{}, fmt.Errorf("reading actor length: %w", err)
		}
		if offset+int(actorLen) > len(data) {
			return Cursor{}, fmt.Errorf("actor data truncated")
		}
		actor := make([]byte, actorLen)
		copy(actor, data[offset:offset+int(actorLen)])
		offset += int(actorLen)

		var ctr uint64
		ctr, offset, err = encoding.ReadULEB128(data, offset)
		if err != nil {
			return Cursor{}, fmt.Errorf("reading counter: %w", err)
		}

		if offset >= len(data) {
			return Cursor{}, fmt.Errorf("missing move tag")
		}
		moveTag := data[offset]
		var move MoveCursor
		switch moveTag {
		case cursorMoveBeforeTag:
			move = MoveBefore
		case cursorMoveAfterTag:
			move = MoveAfter
		default:
			return Cursor{}, fmt.Errorf("unknown move tag %d", moveTag)
		}

		return Cursor{
			Kind:       CursorOp,
			Counter:    ctr,
			Actor:      ActorId(actor),
			MoveCursor: move,
		}, nil
	default:
		return Cursor{}, fmt.Errorf("unknown cursor tag %d", tag)
	}
}

// parseCursorV0 handles the legacy v0 format (no start/end, always MoveAfter).
func parseCursorV0(data []byte) (Cursor, error) {
	offset := 0
	actorLen, offset, err := encoding.ReadULEB128(data, offset)
	if err != nil {
		return Cursor{}, fmt.Errorf("reading actor length: %w", err)
	}
	if offset+int(actorLen) > len(data) {
		return Cursor{}, fmt.Errorf("actor data truncated")
	}
	actor := make([]byte, actorLen)
	copy(actor, data[offset:offset+int(actorLen)])
	offset += int(actorLen)

	ctr, _, err := encoding.ReadULEB128(data, offset)
	if err != nil {
		return Cursor{}, fmt.Errorf("reading counter: %w", err)
	}

	return Cursor{
		Kind:       CursorOp,
		Counter:    ctr,
		Actor:      ActorId(actor),
		MoveCursor: MoveAfter,
	}, nil
}

// String returns the string representation of a cursor.
func (c Cursor) String() string {
	switch c.Kind {
	case CursorStart:
		return "s"
	case CursorEnd:
		return "e"
	case CursorOp:
		prefix := ""
		if c.MoveCursor == MoveBefore {
			prefix = "-"
		}
		return fmt.Sprintf("%s%d@%s", prefix, c.Counter, c.Actor.Hex())
	default:
		return ""
	}
}

// CursorFromString parses a cursor from its string representation.
func CursorFromString(s string) (Cursor, error) {
	if s == "s" {
		return StartCursor(), nil
	}
	if s == "e" {
		return EndCursor(), nil
	}

	move := MoveAfter
	rest := s
	if strings.HasPrefix(s, "-") {
		move = MoveBefore
		rest = s[1:]
	}

	at := strings.IndexByte(rest, '@')
	if at < 0 {
		return Cursor{}, fmt.Errorf("invalid cursor format: %q", s)
	}

	ctr, err := strconv.ParseUint(rest[:at], 10, 64)
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid cursor counter: %w", err)
	}

	actor, err := ActorIdFromHex(rest[at+1:])
	if err != nil {
		return Cursor{}, fmt.Errorf("invalid cursor actor: %w", err)
	}

	return Cursor{
		Kind:       CursorOp,
		Counter:    ctr,
		Actor:      actor,
		MoveCursor: move,
	}, nil
}

// ActorIdFromHex parses an actor ID from a hex string.
func ActorIdFromHex(s string) (ActorId, error) {
	if len(s)%2 != 0 {
		return nil, fmt.Errorf("odd-length hex string")
	}
	b := make([]byte, len(s)/2)
	for i := 0; i < len(s); i += 2 {
		hi := unhex(s[i])
		lo := unhex(s[i+1])
		if hi == 0xFF || lo == 0xFF {
			return nil, fmt.Errorf("invalid hex character at position %d", i)
		}
		b[i/2] = hi<<4 | lo
	}
	return ActorId(b), nil
}

func unhex(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return 0xFF
	}
}

// GetCursor returns a cursor for the element at the given index in a sequence.
func (d *Doc) GetCursor(obj ObjId, index uint64, move MoveCursor) (Cursor, error) {
	objType, err := d.opSet.GetObjType(obj)
	if err != nil {
		return Cursor{}, fmt.Errorf("getting object type: %w", err)
	}
	if !objType.IsSequence() {
		return Cursor{}, fmt.Errorf("%w: cursor requires sequence object", ErrTypeMismatch)
	}

	elements := d.opSet.VisibleListElements(obj)
	if index >= uint64(len(elements)) {
		return Cursor{}, fmt.Errorf("index %d out of range (len=%d)", index, len(elements))
	}

	elem := elements[index]
	actorIdx := elem.Op.ID.ActorIdx
	var actor ActorId
	if int(actorIdx) < len(d.actors) {
		actor = d.actors[actorIdx]
	}

	return Cursor{
		Kind:       CursorOp,
		Counter:    elem.Op.ID.Counter,
		Actor:      actor,
		MoveCursor: move,
	}, nil
}

// CursorPosition represents the resolved position of a cursor.
type CursorPosition struct {
	IsStart bool
	IsEnd   bool
	Index   uint64
}

// GetCursorPosition resolves a cursor to a position in the sequence.
func (d *Doc) GetCursorPosition(obj ObjId, cursor Cursor) (CursorPosition, error) {
	objType, err := d.opSet.GetObjType(obj)
	if err != nil {
		return CursorPosition{}, fmt.Errorf("getting object type: %w", err)
	}
	if !objType.IsSequence() {
		return CursorPosition{}, fmt.Errorf("%w: cursor requires sequence object", ErrTypeMismatch)
	}

	elements := d.opSet.VisibleListElements(obj)

	switch cursor.Kind {
	case CursorStart:
		return CursorPosition{IsStart: true, Index: 0}, nil
	case CursorEnd:
		return CursorPosition{IsEnd: true, Index: uint64(len(elements))}, nil
	case CursorOp:
		return d.resolveCursorOp(obj, cursor, elements), nil
	default:
		return CursorPosition{}, fmt.Errorf("unknown cursor kind")
	}
}

func (d *Doc) resolveCursorOp(obj ObjId, cursor Cursor, elements []opset.ListElement) CursorPosition {
	// Find the actor index for this cursor's actor
	actorIdx := int32(-1)
	for i, a := range d.actors {
		if a.Compare(cursor.Actor) == 0 {
			actorIdx = int32(i)
			break
		}
	}

	targetId := types.OpId{Counter: cursor.Counter, ActorIdx: uint32(actorIdx)}

	// Look for the element
	for i, elem := range elements {
		if elem.ElemID == targetId {
			return CursorPosition{Index: uint64(i)}
		}
	}

	// Element not found (was deleted). Resolve based on MoveCursor.
	// Find where the element would have been by scanning all ops
	allOps := d.opSet.OpsForObj(obj)

	if cursor.MoveCursor == MoveBefore {
		// Find last visible element before this one
		for i := len(elements) - 1; i >= 0; i-- {
			if elements[i].ElemID.Compare(targetId) < 0 {
				return CursorPosition{Index: uint64(i)}
			}
		}
		return CursorPosition{Index: 0}
	}

	// MoveAfter: find first visible element after this one
	_ = allOps
	for i, elem := range elements {
		if elem.ElemID.Compare(targetId) > 0 {
			return CursorPosition{Index: uint64(i)}
		}
	}
	return CursorPosition{Index: uint64(len(elements))}
}
