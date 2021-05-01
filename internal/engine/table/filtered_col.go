package table

type filteredColTable struct {
	underlying Table
	keep       func(int, Col) bool
}

// NewFilteredCol returns a new table that will filter columns from the given underlying
// table.
//
//	tbl := getSevenColTable()
//	len(tbl.Cols()) == 7
//	newTbl = table.NewFilteredCol(tbl, func(i int, c Col) bool { return i > 0 })
//	len(newTbl.Cols()) == 6 // first column is not present anymore
func NewFilteredCol(underlying Table, keep func(int, Col) bool) Table {
	return filteredColTable{
		underlying: underlying,
		keep:       keep,
	}
}

// Cols returns the filtered columns as a slice.
func (t filteredColTable) Cols() ([]Col, error) {
	underlyingCols, err := t.underlying.Cols()
	if err != nil {
		return nil, err
	}

	var cols []Col
	for i, col := range underlyingCols {
		if t.keep(i, col) {
			cols = append(cols, col)
		}
	}
	return cols, nil
}

// Rows returns a row iterator that will obtain rows from the underlying
// table and filter its columns. The result is a row iterator that only
// returns rows with columns that this filtered table wants to keep.
func (t filteredColTable) Rows() (RowIterator, error) {
	return newFilteredColIterator(t.underlying, t.keep)
}
