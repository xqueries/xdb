package table

import (
	"bytes"
	"fmt"
	"strings"
	"text/tabwriter"
)

var (
	// EmptyTable is the empty table, with 0 cols and 0 rows.
	EmptyTable = Table{
		Cols: make([]Col, 0),
		Rows: make([]Row, 0),
	}
)

// Table is a one-dimensional collection of Rows.
type Table struct {
	Cols []Col
	Rows []Row
}

// RemoveColumnByQualifiedName will remove the first column with the given
// qualified name from the table, and return the new table. The original table
// will not be modified. If no such column exists, the original table is
// returned.
func (t Table) RemoveColumnByQualifiedName(qualifiedName string) Table {
	index := -1
	for i, col := range t.Cols {
		if col.QualifiedName == qualifiedName {
			index = i
			break
		}
	}
	if index != -1 {
		return t.RemoveColumn(index)
	}
	return t
}

// HasColumn inspects the table's columns and determines whether the table has
// any column, that has the given name as qualified name OR as alias.
func (t Table) HasColumn(qualifiedNameOrAlias string) bool {
	for _, col := range t.Cols {
		if col.QualifiedName == qualifiedNameOrAlias || col.Alias == qualifiedNameOrAlias {
			return true
		}
	}
	return false
}

// RemoveColumn works on a copy of the table, and removes the column with the
// given index from the copy. After removal, the copy is returned.
func (t Table) RemoveColumn(index int) Table {
	t.Cols = append(t.Cols[:index], t.Cols[index+1:]...)
	for i := range t.Rows {
		t.Rows[i].Values = append(t.Rows[i].Values[:index], t.Rows[i].Values[index+1:]...)
	}
	return t
}

// FilterRows filteres this table's rows according to the given keep function.
// Rows for which the given function returns true and no error, will be copied
// over to a new table, which will then be returned. The keep function is fed
// with one row at a time, but always all columns from the original table, to
// facilitate checking values by index.
func (t Table) FilterRows(keep func(RowWithColInfo) (bool, error)) (Table, error) {
	newTable := Table{
		Cols: t.Cols,
	}
	for _, row := range t.Rows {
		shouldKeep, err := keep(RowWithColInfo{
			Cols: t.Cols,
			Row:  row,
		})
		if err != nil {
			return Table{}, err
		}
		if shouldKeep {
			newTable.Rows = append(newTable.Rows, row)
		}
	}
	return newTable, nil
}

// Validate checks, whether all values in the table are of the correct type that the table indicates.
// If not, an error is returned and the validation is aborted.
func (t Table) Validate() error {
	for rowIndex, row := range t.Rows {
		for colIndex, value := range row.Values {
			if !value.Is(t.Cols[colIndex].Type) {
				return fmt.Errorf("invalid value type in row %v, expected %v (from table) but got %v (from row)", rowIndex, t.Cols[colIndex].Type, value.Type())
			}
		}
	}
	return nil
}

func (t Table) String() string {
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 1, 3, ' ', 0)

	var colNames []string
	for _, col := range t.Cols {
		colName := col.QualifiedName
		if col.Alias != "" {
			colName = col.Alias
		}
		colNames = append(colNames, colName+" ("+col.Type.String()+")")
	}
	_, _ = fmt.Fprintln(w, strings.Join(colNames, "\t"))

	for _, row := range t.Rows {
		var strVals []string
		for i := 0; i < len(row.Values); i++ {
			strVals = append(strVals, row.Values[i].String())
		}
		_, _ = fmt.Fprintln(w, strings.Join(strVals, "\t"))
	}
	_ = w.Flush()
	return buf.String()
}
