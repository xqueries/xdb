package inspect

import (
	"github.com/xqueries/xdb/internal/engine/table"

	"github.com/xqueries/xdb/internal/inspect/format"
)

func (i *Inspector) ProcessTablesCommand() (string, error) {
	return format.Tables(i.info.TableNames()), nil
}

// ProcessTableCommand responds to the command related to a table.
// The response is a pre-formatted string which can be printed
// on a CLI.
//
func (i *Inspector) ProcessTableCommand(arg string) (string, error) {
	tbl, err := i.e.LoadTable(arg)
	if err != nil {
		return "", err
	}

	rowItr, err := tbl.Rows()
	if err != nil {
		return "", err
	}

	// Extract rows from the table.
	var rows []table.Row
	for {
		row, err := rowItr.Next()
		if err == table.ErrEOT {
			break
		}
		if err != nil {
			return "", err
		}
		rows = append(rows, row)
	}

	// Get column info of the rows.
	var rowsWithColInfo []table.RowWithColInfo
	for _, v := range rows {
		rowsWithColInfo = append(rowsWithColInfo, table.NewRowWithImplicitColData(v.Values))
	}

	return format.Table(rowsWithColInfo), nil
}
