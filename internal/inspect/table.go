package inspect

import (
	"fmt"

	"github.com/xqueries/xdb/internal/inspect/format"
)

// ProcessTableCommand responds to the command related to a table.
// The response is a pre-formatted string which can be printed
// on a CLI.
func (i *Inspector) ProcessTableCommand(arg string) (string, error) {
	table, err := i.e.LoadTable(arg)
	if err != nil {
		return "", err
	}

	rowItr, err := table.Rows()
	if err != nil {
		return "", err
	}

	for {
		row, err := rowItr.Next()
		if err != nil {
			return "", err
		}
		fmt.Println(row)
	}

	return format.Table(table)
}
