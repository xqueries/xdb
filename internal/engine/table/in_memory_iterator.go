package table

type inMemoryRowIterator struct {
	index int
	rows  []Row
}

// Next returns the next row of this iterator.
func (i *inMemoryRowIterator) Next() (Row, bool) {
	if i.index == len(i.rows) {
		return Row{}, false
	}
	row := i.rows[i.index]
	i.index++
	return row, true
}

// Reset resets the row index to zero, causing Next to return
// the first row on the next call and restarting this iterator.
func (i inMemoryRowIterator) Reset() error {
	i.index = 0
	return nil
}
