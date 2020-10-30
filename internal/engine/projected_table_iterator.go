package engine

import (
	"fmt"
	"sync"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

type projectedTableIterator struct {
	underlying           table.RowIterator
	underlyingColumns    []table.Col
	projectedColumns     []command.Column
	ctx                  ExecutionContext
	e                    Engine
	rowCounter           uint64
	isSingleRowTableOnce *sync.Once
	isSingleRowTable     bool
}

func (t projectedTable) createIterator() (*projectedTableIterator, error) {
	underlyingIterator, err := t.originalTable.Rows()
	if err != nil {
		return nil, err
	}
	return &projectedTableIterator{
		underlying:           underlyingIterator,
		projectedColumns:     t.projectedColumns,
		underlyingColumns:    t.originalTable.Cols(),
		ctx:                  t.ctx,
		e:                    t.e,
		isSingleRowTableOnce: &sync.Once{},
	}, nil
}

// Next returns the next row from this table. It fetches an underlying row and performs the projection
// on it, meaning that columns might be renamed, reordered, added or removed.
func (i *projectedTableIterator) Next() (table.Row, error) {
	if i.isSingleRowTable && i.rowCounter > 0 {
		// we've already returned one row in a single row table, this is the
		// end of this table
		return table.Row{}, table.ErrEOT
	}

	nextUnderlying, err := i.underlying.Next()
	if err != nil {
		nextUnderlying = table.Row{} // this is what we expect right here, but we do this to avoid a warning
		if err != table.ErrEOT {
			return table.Row{}, err
		} else if err == table.ErrEOT && i.rowCounter > 0 {
			// Only allow ErrEOT if there's already been a row returned.
			// If we don't do this, something like `SELECT "a"` wouldn't
			// return any rows, since the underlyingTable is empty.
			return table.Row{}, err
		}
	}

	var vals []types.Value
	for _, col := range i.projectedColumns {
		newCtx := i.ctx.IntermediateRow(table.RowWithColInfo{
			Cols: i.underlyingColumns,
			Row:  nextUnderlying,
		})
		// check if only a single row must be returned
		i.isSingleRowTableOnce.Do(func() {
			i.isSingleRowTable = projectedColumnsImplySingleRowTable(newCtx, i.projectedColumns)
		})

		if name, ok := col.Expr.(command.ColumnReference); ok && name.Name == "*" {
			// add all underlying columns for an asterisk
			vals = append(vals, nextUnderlying.Values...)
		} else {
			val, err := i.e.evaluateExpression(newCtx, col.Expr)
			if err != nil {
				return table.Row{}, fmt.Errorf("evaluate expression: %w", err)
			}
			vals = append(vals, val)
		}
	}

	i.rowCounter++
	return table.Row{Values: vals}, nil
}

// Reset resets this table iterator, causing it to start from row 0 again.
func (i *projectedTableIterator) Reset() error {
	i.isSingleRowTableOnce = &sync.Once{}
	i.isSingleRowTable = false
	return i.underlying.Reset()
}

// projectedColumnsImplySingleRowTable checks whether the projected columns imply, that the resulting
// table should only consist of a single row. This would be the case if they contain constant columns,
// as in
//	SELECT 'a';
// or
//	SELECT MIN(col1) FROM myTable;
// The resulting table then only consists of one row, instead of repeating the same value over and over.
func projectedColumnsImplySingleRowTable(ctx ExecutionContext, columns []command.Column) bool {
	for _, column := range columns {
		switch name := column.Expr.(type) {
		case command.ConstantLiteral:
			// if one projected column is a constant literal, the table is
			// a single row table in any case
			return true
		case command.ConstantLiteralOrColumnReference:
			_, ok := ctx.intermediateRow.ValueForColName(name.ValueOrName)
			return !ok // column found -> not a single row table
		}
	}
	return false
}
