package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func (e Engine) evaluate(ctx ExecutionContext, c command.Command) (table.Table, error) {
	switch cmd := c.(type) {
	case command.List:
		return e.evaluateList(ctx, cmd)
	}
	return table.Table{}, ErrUnimplemented(c)
}

func (e Engine) evaluateList(ctx ExecutionContext, l command.List) (table.Table, error) {
	switch list := l.(type) {
	case command.Values:
		values, err := e.evaluateValues(ctx, list)
		if err != nil {
			return table.Table{}, fmt.Errorf("values: %w", err)
		}
		return values, nil
	case command.Scan:
		scanned, err := e.evaluateScan(ctx, list)
		if err != nil {
			return table.Table{}, fmt.Errorf("scan: %w", err)
		}
		return scanned, nil
	case command.Project:
		return e.evaluateProjection(ctx, list)
	case command.Select:
		return e.evaluateSelection(ctx, list)
	}
	return table.Table{}, ErrUnimplemented(l)
}

func (e Engine) evaluateProjection(ctx ExecutionContext, proj command.Project) (table.Table, error) {
	origin, err := e.evaluateList(ctx, proj.Input)
	if err != nil {
		return table.Table{}, fmt.Errorf("list: %w", err)
	}

	if len(proj.Cols) == 0 {
		e.log.Debug().
			Str("ctx", ctx.String()).
			Msg("projection filters all columns")
		return table.EmptyTable, nil
	}

	var expectedColumnNames []string
	aliases := make(map[string]string)
	for _, col := range proj.Cols {
		// evaluate the column name
		colNameExpr, err := e.evaluateExpression(ctx, col.Column)
		if err != nil {
			return table.Table{}, fmt.Errorf("eval column name: %w", err)
		}
		var colName string
		if colNameExpr.Is(types.String) {
			colName = colNameExpr.(types.StringValue).Value
		} else {
			casted, err := types.String.Cast(colNameExpr)
			if err != nil {
				return table.Table{}, fmt.Errorf("cannot cast %v to %v: %w", colNameExpr.Type(), types.String, err)
			}
			colName = casted.(types.StringValue).Value
		}
		if col.Table != "" {
			colName = col.Table + "." + colName
		}
		if col.Alias != "" {
			aliases[colName] = col.Alias
		}

		expectedColumnNames = append(expectedColumnNames, colName)
	}

	// check if the table actually has all expected columns
	for _, expectedCol := range expectedColumnNames {
		if expectedCol == "*" {
			continue
		}
		if !origin.HasColumn(expectedCol) {
			return table.Table{}, ErrNoSuchColumn(expectedCol)
		}
	}

	// apply aliases
	for i, col := range origin.Cols {
		if alias, ok := aliases[col.QualifiedName]; ok {
			origin.Cols[i].Alias = alias
		}
	}

	var toRemove []string
	for _, col := range origin.Cols {
		found := false
		if len(expectedColumnNames) == 1 && expectedColumnNames[0] == "*" {
			found = true
		} else {
			for _, expectedColumnName := range expectedColumnNames {
				if expectedColumnName == col.QualifiedName {
					found = true
					break
				}
			}
		}
		if !found {
			toRemove = append(toRemove, col.QualifiedName)
		}
	}

	for _, toRemoveCol := range toRemove {
		origin = origin.RemoveColumnByQualifiedName(toRemoveCol)
	}

	return origin, nil
}

func (e Engine) evaluateValues(ctx ExecutionContext, v command.Values) (tbl table.Table, err error) {
	defer e.profiler.Enter("values").Exit()

	var colCnt int
	for _, values := range v.Values {
		rowValues := make([]types.Value, len(values))
		colCnt = len(values)
		for x, value := range values {
			internalValue, err := e.evaluateExpression(ctx, value)
			if err != nil {
				return table.Table{}, fmt.Errorf("expr: %w", err)
			}
			rowValues[x] = internalValue
		}
		tbl.Rows = append(tbl.Rows, table.Row{
			Values: rowValues,
		})
	}

	for i := 1; i <= colCnt; i++ {
		tbl.Cols = append(tbl.Cols, table.Col{
			QualifiedName: fmt.Sprintf("column%d", i),
			Type:          tbl.Rows[0].Values[i-1].Type(),
		})
	}

	return
}

func (e Engine) evaluateScan(ctx ExecutionContext, s command.Scan) (table.Table, error) {
	defer e.profiler.Enter("scan").Exit()

	switch tbl := s.Table.(type) {
	case command.SimpleTable:
		return e.scanSimpleTable(ctx, tbl)
	default:
		return table.Table{}, ErrUnimplemented(fmt.Sprintf("scan %T", tbl))
	}
}

func (e Engine) evaluateSelection(ctx ExecutionContext, sel command.Select) (table.Table, error) {
	origin, err := e.evaluateList(ctx, sel.Input)
	if err != nil {
		return table.Table{}, fmt.Errorf("list: %w", err)
	}

	// filter might have been optimized to constant expression
	if expr, ok := sel.Filter.(command.ConstantBooleanExpr); ok && expr.Value {
		return origin, nil
	}

	switch t := sel.Filter.(type) {
	case command.EqualityExpr:
	default:
		return table.Table{}, fmt.Errorf("cannot use %T as filter", t)
	}

	newTable, err := origin.FilterRows(func(r table.RowWithColInfo) (bool, error) {
		switch filter := sel.Filter.(type) {
		case command.EqualityExpr:
			newCtx := ctx.IntermediateRow(r)
			left, err := e.evaluateExpression(newCtx, filter.Left)
			if err != nil {
				return false, fmt.Errorf("left: %w", err)
			}
			right, err := e.evaluateExpression(newCtx, filter.Right)
			if err != nil {
				return false, fmt.Errorf("right: %w", err)
			}

			return e.cmp(left, right) == cmpEqual, nil
		}
		return false, nil
	})
	if err != nil {
		return table.Table{}, fmt.Errorf("filter: %w", err)
	}

	return newTable, nil
}
