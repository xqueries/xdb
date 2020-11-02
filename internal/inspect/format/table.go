package format

import (
	"fmt"

	prettyTable "github.com/jedib0t/go-pretty/table"
	"github.com/xqueries/xdb/internal/engine/table"
)

func Tables(tableNames []string) string {
	return fmt.Sprint(tableNames)
}

func Table(rows []table.RowWithColInfo) string {
	t := prettyTable.NewWriter()

	columns := extractCols(rows[0].Cols)
	t.AppendHeader(columns)

	for _, v := range rows {
		t.AppendRow(Row(v))
	}

	t.Style().Options.SeparateRows = true
	return t.Render()
}

func Row(row table.RowWithColInfo) prettyTable.Row {
	var rowVals prettyTable.Row
	for _, v := range row.Row.Values {
		rowVals = append(rowVals, v.String())
	}
	return rowVals
}

func extractCols(cols []table.Col) prettyTable.Row {
	var columns prettyTable.Row
	for _, v := range cols {
		columns = append(columns, v.String())
	}
	return columns
}
