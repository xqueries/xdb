package dbfs

// Error is a sentinel error type.
type Error string

func (e Error) Error() string { return string(e) }

const (
	// ErrPageNotExist indicates that the requested page does not exist.
	ErrPageNotExist Error = "page doesn't exist"
)
