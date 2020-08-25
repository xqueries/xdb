package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

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
