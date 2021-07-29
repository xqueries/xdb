package table

type inMemoryRowIterator struct {
	index int
	rows  []Row
}

// Next returns the next row of this iterator.
func (i *inMemoryRowIterator) Next() (Row, error) {
	if i.index == len(i.rows) {
		return Row{}, ErrEOT
	}
	row := i.rows[i.index]
	i.index++
	return row, nil
}

// Reset resets the row index to zero, causing Next to return
// the first row on the next call and restarting this iterator.
func (i *inMemoryRowIterator) Reset() error {
	i.index = 0
	return nil
}

func (i inMemoryRowIterator) Close() error {
	return nil
}
