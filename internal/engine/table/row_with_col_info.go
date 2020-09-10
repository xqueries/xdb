package table

import (
	"strconv"

	"github.com/xqueries/xdb/internal/engine/types"
)

// RowWithColInfo is a row with col information available.
type RowWithColInfo struct {
	Cols []Col
	Row
}

// NewRowWithImplicitColData creates a new RowWithColInfo and implies the column information
// from the given values. That is, type information is extracted from the values, and column
// qualified names are of the form column<colIndex>, where colIndex is one-based.
// This results in generated column names such as column1, column5, column26 etc.
func NewRowWithImplicitColData(values []types.Value) RowWithColInfo {
	cols := make([]Col, len(values))
	for i, value := range values {
		cols[i] = Col{
			QualifiedName: "column" + strconv.Itoa(i+1),
			Type:          value.Type(),
		}
	}
	return RowWithColInfo{
		Cols: cols,
		Row: Row{
			Values: values,
		},
	}
}

// ValueForColName checks if this row has a value for the given col name.
// It has such a value, if any cols QualifiedName or Alias matches the given
// argument. If no such value is present, false is returned.
func (r RowWithColInfo) ValueForColName(colName string) (types.Value, bool) {
	for i, col := range r.Cols {
		if col.QualifiedName == colName ||
			col.Alias == colName {
			return r.Values[i], true
		}
	}
	return nil, false
}
