package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func (e Engine) evaluateValues(ctx ExecutionContext, v command.Values) (table.Table, error) {
	defer e.profiler.Enter("values").Exit()

	var rows []table.Row
	var cols []table.Col

	var colCnt int
	for _, values := range v.Values {
		rowValues := make([]types.Value, len(values))
		colCnt = len(values)
		for x, value := range values {
			internalValue, err := e.evaluateExpression(ctx, value)
			if err != nil {
				return nil, fmt.Errorf("expr: %w", err)
			}
			rowValues[x] = internalValue
		}
		rows = append(rows, table.Row{
			Values: rowValues,
		})
	}

	if len(rows) == 0 {
		return table.Empty, nil
	}

	for i := 1; i <= colCnt; i++ {
		cols = append(cols, table.Col{
			QualifiedName: fmt.Sprintf("column%d", i),
			Type:          rows[0].Values[i-1].Type(),
		})
	}

	return table.NewInMemory(cols, rows), nil
}
