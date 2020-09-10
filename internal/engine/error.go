package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/engine/types"
)

// Error is a sentinel error.
type Error string

func (e Error) Error() string { return string(e) }

const (
	// ErrUnsupported indicates that a requested feature is explicitely not
	// supported. This is different from ErrUnimplemented, since
	// ErrUnimplemented indicates, that the feature has not been implemented
	// yet, while ErrUnsupported indicates, that the feature is intentionally
	// unimplemented.
	ErrUnsupported Error = "unsupported"
	// ErrAlreadyExists indicates, that whatever was meant to be created, already
	// exists, and therefore, the new thing cannot be created.
	ErrAlreadyExists Error = "already exists"
)

// ErrNoSuchFunction returns an error indicating that a function with the given
// name can not be found.
func ErrNoSuchFunction(name string) Error {
	return Error(fmt.Sprintf("no function for name %v(...)", name))
}

// ErrUncomparable returns an error indicating that the given type does not
// implement the types.Comparator interface, and thus, values of that type
// cannot be compared.
func ErrUncomparable(t types.Type) Error {
	return Error(fmt.Sprintf("type %v is not comparable", t))
}

// ErrUnimplemented returns an error indicating a missing implementation for the
// requested feature. It may be implemented in the next version.
func ErrUnimplemented(what interface{}) Error {
	return Error(fmt.Sprintf("'%v' is not implemented", what))
}

// ErrNoSuchColumn returns an error indicating that a requested column is not
// contained in the current result table.
func ErrNoSuchColumn(name string) Error {
	return Error(fmt.Sprintf("no column with name or alias '%s'", name))
}
