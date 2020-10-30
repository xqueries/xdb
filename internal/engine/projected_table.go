package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

// projectedTable is a table, which projects from an underlying table.
// This means, this table may reorder, rename, remove or add columns.
type projectedTable struct {
	columns          []table.Col
	originalTable    table.Table
	projectedColumns []command.Column
	ctx              ExecutionContext
	e                Engine
}

func (e Engine) newProjectedTable(ctx ExecutionContext, originalTable table.Table, columnExpressions []command.Column) (projectedTable, error) {
	tbl := projectedTable{
		originalTable:    originalTable,
		projectedColumns: columnExpressions,
		ctx:              ctx,
		e:                e,
	}

	// compute the column names
	var cols []table.Col
	for i, colNameExpr := range columnExpressions {
		if ref, ok := colNameExpr.Expr.(command.ColumnReference); ok {
			if ref.Name == "*" {
				cols = append(cols, originalTable.Cols()...)
			} else {
				foundCol, ok := table.FindColumnForNameOrAlias(originalTable, ref.Name)
				if !ok {
					return projectedTable{}, ErrNoSuchColumn(ref.Name)
				}
				cols = append(cols, foundCol)
			}
		} else if litOrRef, ok := colNameExpr.Expr.(command.ConstantLiteralOrColumnReference); ok {
			foundCol, ok := table.FindColumnForNameOrAlias(originalTable, litOrRef.ValueOrName)
			if ok {
				cols = append(cols, foundCol)
			} else {
				evaluatedName, err := e.evaluateExpression(ctx, colNameExpr.Expr)
				if err != nil {
					return projectedTable{}, fmt.Errorf("typeof colName: %w", err)
				}
				cols = append(cols, table.Col{
					QualifiedName: litOrRef.ValueOrName,
					Alias:         colNameExpr.Alias,
					Type:          evaluatedName.Type(),
				})
			}
		} else {
			colName, err := e.evaluateExpression(ctx, colNameExpr.Expr)
			if err != nil {
				return projectedTable{}, fmt.Errorf("col name: %w", err)
			}
			if !colName.Is(types.String) {
				colNameStr, err := types.String.Cast(colName)
				if err != nil {
					return projectedTable{}, fmt.Errorf("cast col name to string: %w", err)
				}
				cols = append(cols, table.Col{
					QualifiedName: colNameStr.(types.StringValue).Value,
					Alias:         colNameExpr.Alias,
					Type:          colName.Type(),
				})
			} else {
				cols = append(cols, table.Col{
					QualifiedName: colName.(types.StringValue).Value,
					Alias:         colNameExpr.Alias,
					Type:          colName.Type(),
				})
			}
		}
		cols[i].Alias = colNameExpr.Alias
	}

	tbl.columns = cols
	return tbl, nil
}

// Cols returns the columns of the projected table.
func (t projectedTable) Cols() []table.Col {
	return t.columns
}

// Rows returns a row iterator of the projected table. Use it to read rows one by one.
func (t projectedTable) Rows() (table.RowIterator, error) {
	return t.createIterator()
}
