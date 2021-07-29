package transaction

import "io"

// Manager describes a component that can start and
// process transactions. A manager is used to start
// a transaction, which will be used to store modifications
// to the database. The transaction only describe the
// changes, they do not perform them.
//
// The Manager is responsible to apply the described changes.
// If this is not possible, NO CHANGE of the transaction must
// be applied.
//
// A Manager is also responsible for keeping track of pending
// transactions. Upon close, these transactions must be
// persisted, either in the database, in a journal or similar.
// The exact way of persisting these pending changes is up
// to the implementation.
// The constructor of a Manager should read persisted, still
// pending changes.
type Manager interface {
	io.Closer

	// Start a new transaction that will be pending in this manager
	// until it is either committed or rolled back. This means, that
	// it will be persisted if the Manager is closed before the
	// transaction finishes.
	Start() (*TX, error)
	// Commit will apply all changes from the transaction to the database,
	// or return an error and not apply any changes.
	Commit(*TX) error
	// Rollback will abort the transaction. No changes will be applied
	// and the transaction will not be considered 'pending' anymore.
	Rollback(*TX) error
}
