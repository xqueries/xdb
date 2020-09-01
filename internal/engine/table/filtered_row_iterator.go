package table

type filteredRowIterator struct {
	origin     Table
	keep       func(RowWithColInfo) bool
	underlying RowIterator
}

func newFilteredRowIterator(origin Table, keep func(RowWithColInfo) bool) (*filteredRowIterator, error) {
	rows, err := origin.Rows()
	if err != nil {
		return nil, err
	}
	return &filteredRowIterator{
		origin:     origin,
		keep:       keep,
		underlying: rows,
	}, nil
}

// Next returns the next not filtered row.
func (i filteredRowIterator) Next() (Row, bool) {
	for {
		next, ok := i.underlying.Next()
		if !ok {
			return Row{}, false
		}
		if i.keep(RowWithColInfo{
			Cols: i.origin.Cols(),
			Row:  next,
		}) {
			return next, true
		}
	}
}

// Reset resets this row iterator by obtaining a new row iterator
// from the underlying table.
func (i *filteredRowIterator) Reset() error {
	rows, err := i.origin.Rows()
	if err != nil {
		return err
	}
	i.underlying = rows
	return nil
}
