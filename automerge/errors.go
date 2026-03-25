package automerge

import "errors"

var (
	// ErrNotFound indicates a value was not found at the given path.
	ErrNotFound = errors.New("not found")

	// ErrInvalidObject indicates an invalid object ID was used.
	ErrInvalidObject = errors.New("invalid object")

	// ErrTypeMismatch indicates a type mismatch (e.g., map operation on a list).
	ErrTypeMismatch = errors.New("type mismatch")

	// ErrInvalidDocument indicates the document data is invalid.
	ErrInvalidDocument = errors.New("invalid document")
)
