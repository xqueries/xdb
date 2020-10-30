package table

import (
	"bytes"
	"fmt"
	"strings"
	"text/tabwriter"
)

// ToString converts the given table to a string. To do this, it calls Table.Rows exactly once
// and drains the obtained RowIterator. The resulting string representation is a formatted
// table, where the column headers contain the type name of the column, and aliases are used
// over plain column names, if present.
//
//	col1 (Integer)   col2 (String)
//	1                foobar
//	2                snafu
func ToString(tbl Table) (string, error) {
	if tbl == nil {
		return "", fmt.Errorf("table is nil")
	}

	if stringer, ok := tbl.(fmt.Stringer); ok {
		return stringer.String(), nil
	}

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 1, 3, ' ', 0)

	var colNames []string
	for _, col := range tbl.Cols() {
		colName := col.QualifiedName
		if col.Alias != "" {
			colName = col.Alias
		}
		colNames = append(colNames, colName+" ("+col.Type.String()+")")
	}
	_, _ = fmt.Fprintln(w, strings.Join(colNames, "\t"))

	iterator, err := tbl.Rows()
	if err != nil {
		_ = w.Flush()
		return buf.String(), err
	}
	for {
		next, err := iterator.Next()
		if err == ErrEOT {
			break
		} else if err != nil {
			_ = w.Flush()
			return buf.String(), fmt.Errorf("next: %w", err)
		}

		var strVals []string
		for i := 0; i < len(next.Values); i++ {
			strVals = append(strVals, next.Values[i].String())
		}
		_, _ = fmt.Fprintln(w, strings.Join(strVals, "\t"))
	}
	_ = w.Flush()
	return buf.String(), nil
}
