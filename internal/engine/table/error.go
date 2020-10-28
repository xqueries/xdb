package table

// Error is a sentinel error.
type Error string

func (e Error) Error() string { return string(e) }

const (
	// ErrEOT indicates, that a table iterator is fully drained and will not yield
	// any more results.
	ErrEOT Error = "end of table"
)
