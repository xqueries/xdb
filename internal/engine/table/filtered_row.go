package table

type filteredRowTable struct {
	underlying Table
	keep       func(RowWithColInfo) (bool, error)
}

// NewFilteredRow returns a new table that can filter rows from the given
// underlying table.
func NewFilteredRow(underlying Table, keep func(RowWithColInfo) (bool, error)) Table {
	return filteredRowTable{
		underlying: underlying,
		keep:       keep,
	}
}

// Cols returns the columns of the underlying table.
func (t filteredRowTable) Cols() []Col {
	return t.underlying.Cols()
}

// Rows returns a row iterator that will only return not filtered
// rows.
func (t filteredRowTable) Rows() (RowIterator, error) {
	return newFilteredRowIterator(t.underlying, t.keep)
}
