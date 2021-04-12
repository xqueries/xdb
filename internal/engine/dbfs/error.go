package dbfs

type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrPageNotExist Error = "page doesn't exist"
)
