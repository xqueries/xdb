package engine

import "github.com/xqueries/xdb/internal/id"

type Transaction struct {
	id id.ID
}

func (tx Transaction) ID() id.ID {
	return tx.id
}
