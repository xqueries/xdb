package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func (e Engine) evaluateProjection(ctx ExecutionContext, proj command.Project) (table.Table, error) {
	defer e.profiler.Enter("projection").Exit()

	origin, err := e.evaluateList(ctx, proj.Input)
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}

	if len(proj.Cols) == 0 {
		e.log.Debug().
			Str("ctx", ctx.String()).
			Msg("projection filters all columns")
		return table.Empty, nil
	}

	var expectedColumnNames []string
	aliases := make(map[string]string)
	for _, col := range proj.Cols {
		// evaluate the column name
		colNameExpr, err := e.evaluateExpression(ctx, col.Name)
		if err != nil {
			return nil, fmt.Errorf("eval column name: %w", err)
		}
		var colName string
		if colNameExpr.Is(types.String) {
			colName = colNameExpr.(types.StringValue).Value
		} else {
			casted, err := types.String.Cast(colNameExpr)
			if err != nil {
				return nil, fmt.Errorf("cannot cast %v to %v: %w", colNameExpr.Type(), types.String, err)
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
		var found bool
		for _, col := range origin.Cols() {
			if col.QualifiedName == expectedCol || col.Alias == expectedCol {
				found = true
				break
			}
		}
		if !found {
			return nil, ErrNoSuchColumn(expectedCol)
		}
	}

	// apply aliases
	for i, col := range origin.Cols() {
		if alias, ok := aliases[col.QualifiedName]; ok {
			origin.Cols()[i].Alias = alias
		}
	}

	var toRemove []string
	for _, col := range origin.Cols() {
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

	return table.NewFilteredCol(origin, func(index int, col table.Col) bool {
		defer e.profiler.Enter("projection (lazy)").Exit()
		for _, s := range toRemove {
			if s == col.QualifiedName {
				return false
			}
		}
		return true
	}), nil
}
