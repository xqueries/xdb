package transaction

//go:generate stringer -trimprefix State -type State

// State describes the current state of a transaction, and
// implies whether or not the transaction may be modified
// any more.
type State uint8

const (
	// StateUnknown indicates that the state is unknown and/or invalid
	// and/or not set yet.
	StateUnknown State = iota
	// StatePending indicates that a transaction is still in use and
	// may be modified further.
	StatePending
	// StateCommitted indicates that the transaction was committed to
	// secondary storage, and that it can not be modified any further.
	StateCommitted
	// StateRolledBack indicates that the transaction was aborted and
	// no changes were persisted. The transaction can not be modified
	// any further.
	StateRolledBack
)
