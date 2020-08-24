package engine

import (
	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
)

func (e Engine) scanSimpleTable(ctx ExecutionContext, tbl command.SimpleTable) (table.Table, error) {
	return table.Table{}, ErrUnimplemented("scan simple table")
}
