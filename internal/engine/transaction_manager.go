package engine

import "io"

type TransactionManager interface {
	io.Closer

	Start() (*Transaction, error)
	Submit(*Transaction) error
}
