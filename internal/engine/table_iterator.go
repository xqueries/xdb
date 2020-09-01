package engine

import (
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/storage/page"
	"github.com/xqueries/xdb/internal/engine/table"
)

type tableRowIterator struct {
	profiler *profile.Profiler

	cols     []table.Col
	dataPage *page.Page

	index int
	slots []page.Slot
}

func newTableRowIterator(profiler *profile.Profiler, cols []table.Col, dataPage *page.Page) *tableRowIterator {
	return &tableRowIterator{
		profiler: profiler,
		cols:     cols,
		dataPage: dataPage,
	}
}

// Next returns the next row of this table iterator.
func (i *tableRowIterator) Next() (table.Row, bool) {
	i.profiler.Enter("next row").Exit()

	if i.slots == nil {
		i.slots = i.dataPage.OccupiedSlots()
	}
	if i.index >= len(i.slots) {
		return table.Row{}, false
	}

	cell := i.dataPage.CellAt(i.slots[i.index]).(page.RecordCell)
	i.index++
	row, err := deserializeRow(i.cols, cell.Record)
	if err != nil {
		return table.Row{}, false
	}
	return row, true
}

// Reset makes this iterator start over from the first row.
func (i *tableRowIterator) Reset() error {
	i.index = 0
	return nil
}
