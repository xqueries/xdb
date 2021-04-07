package table

import (
	"sort"

	"github.com/xqueries/xdb/internal/engine/types"
)

type filteredColIterator struct {
	origin      Table
	keepIndices []int
	underlying  RowIterator
}

func newFilteredColIterator(origin Table, keep func(int, Col) bool) (*filteredColIterator, error) {
	var keepIndices []int
	for i, col := range origin.Cols() {
		if keep(i, col) {
			keepIndices = append(keepIndices, i)
		}
	}
	rows, err := origin.Rows()
	if err != nil {
		return nil, err
	}
	return &filteredColIterator{
		origin:      origin,
		keepIndices: keepIndices,
		underlying:  rows,
	}, nil
}

// Next returns the next row of this table iterator.
func (i filteredColIterator) Next() (Row, error) {
	var vals []types.Value
	next, err := i.underlying.Next()
	if err != nil {
		return Row{}, err
	}
	for colIndex, value := range next.Values {
		result := sort.SearchInts(i.keepIndices, colIndex)
		if result < len(i.keepIndices) && colIndex == i.keepIndices[result] {
			vals = append(vals, value)
		}
	}
	return Row{
		Values: vals,
	}, nil
}

// Reset makes this iterator start over from the first row.
func (i *filteredColIterator) Reset() error {
	rows, err := i.origin.Rows()
	if err != nil {
		return err
	}
	i.underlying = rows
	return nil
}

func (i *filteredColIterator) Close() error {
	return i.underlying.Close()
}
