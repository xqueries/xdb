package table

type filteredRowIterator struct {
	origin     Table
	keep       func(RowWithColInfo) (bool, error)
	underlying RowIterator
}

func newFilteredRowIterator(origin Table, keep func(RowWithColInfo) (bool, error)) (*filteredRowIterator, error) {
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
func (i filteredRowIterator) Next() (Row, error) {
	for {
		next, err := i.underlying.Next()
		if err != nil {
			return Row{}, err
		}
		if ok, err := i.keep(RowWithColInfo{
			Cols: i.origin.Cols(),
			Row:  next,
		}); err != nil {
			return Row{}, err
		} else if ok {
			return next, nil
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

func (i *filteredRowIterator) Close() error {
	return i.underlying.Close()
}
