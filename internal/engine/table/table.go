package table

var (
	// Empty is an empty table with no cols and no rows.
	Empty Table = inMemoryTable{
		cols: []Col{},
		rows: []Row{},
	}
)

// Table describes a collection of rows, where each row consists of multiple columns.
// A column has a type, and that information is carried separately in the table.
// A table has a row iterator, which can be used to obtain all rows of the table in
// sequence. Multiple calls to RowIterator must result in different iterators.
type Table interface {
	// Cols returns all column information of this table.
	Cols() []Col
	// Rows returns a resettable row iterator, which can be used to iterate over all
	// rows in this table. Multiple calls to this method result in different row iterator
	// objects.
	Rows() (RowIterator, error)
}

// RowIterator is an iterator that can be reset, which results in Next obtaining the rows
// in the beginning again.
type RowIterator interface {
	Next() (Row, bool)
	Reset() error
}
