package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
)

func (e Engine) evaluateScan(s command.Scan) (table.Table, error) {
	defer e.profiler.Enter("scan").Exit()

	switch tbl := s.Table.(type) {
	case command.SimpleTable:
		return e.scanSimpleTable(tbl)
	default:
		return nil, ErrUnimplemented(fmt.Sprintf("scan %T", tbl))
	}
}

func (e Engine) scanSimpleTable(tbl command.SimpleTable) (table.Table, error) {
	return e.LoadTable(tbl.QualifiedName())
}
