package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
)

func (e Engine) evaluateInsert(ctx ExecutionContext, c command.Insert) (table.Table, error) {
	tbl, err := e.LoadTable(c.Table.QualifiedName())
	if err != nil {
		return nil, fmt.Errorf("load table: %w", err)
	}
	inserter, ok := tbl.(Inserter)
	if !ok {
		return nil, fmt.Errorf("table %v is not insertable", c.Table.QualifiedName())
	}

	if len(c.Cols) != 0 {
		return nil, fmt.Errorf("explicit insert projectedColumns: %w", ErrUnsupported)
	}

	insertInput, err := e.evaluateList(ctx, c.Input)
	if err != nil {
		return nil, fmt.Errorf("insert input: %w", err)
	}

	inputRows, err := insertInput.Rows()
	if err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	for {
		next, err := inputRows.Next()
		if err == table.ErrEOT {
			break
		} else if err != nil {
			return nil, err
		}
		if err := inserter.Insert(next); err != nil {
			return nil, fmt.Errorf("insert: %w", err)
		}
	}
	return table.Empty, nil
}
