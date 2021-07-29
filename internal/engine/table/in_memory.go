package table

// Table is a one-dimensional collection of Rows and Cols. This table just holds
// data and is in no way related to any storage component.
type inMemoryTable struct {
	cols []Col
	rows []Row
}

// NewInMemory returns a new in-memory table, consisting of the given cols and rows.
func NewInMemory(cols []Col, rows []Row) Table {
	return inMemoryTable{
		cols: cols,
		rows: rows,
	}
}

// Cols returns the cols of this table.
func (t inMemoryTable) Cols() ([]Col, error) {
	return t.cols, nil
}

// Rows returns an in-memory row iterator that is iterating over
// this table's rows.
func (t inMemoryTable) Rows() (RowIterator, error) {
	return &inMemoryRowIterator{rows: t.rows}, nil
}
