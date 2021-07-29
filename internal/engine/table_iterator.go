package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/engine/page"
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/table"
)

type tableRowIterator struct {
	profiler *profile.Profiler

	table *Table
	cols  []table.Col

	pages            []page.ID
	currentPageIndex int
	currentPage      *page.Page

	slots       []page.Slot
	currentSlot int
}

func newTableRowIterator(tbl *Table) (*tableRowIterator, error) {
	cols, err := tbl.Cols()
	if err != nil {
		return nil, fmt.Errorf("cols: %w", err)
	}
	pages, err := tbl.tx.ExistingDataPagesForTable(tbl.name)
	if err != nil {
		return nil, fmt.Errorf("data pages: %w", err)
	}
	return &tableRowIterator{
		table: tbl,
		cols:  cols,
		pages: pages,
	}, nil
}

// Next returns the next row of this table iterator.
func (i *tableRowIterator) Next() (table.Row, error) {
	i.profiler.Enter("next row").Exit()

	tx := i.table.tx

	if len(i.pages) == 0 {
		return table.Row{}, table.ErrEOT
	}

start:
	// if the current page index is higher than or equal to the amount of pages that exist, we are done
	if i.currentPageIndex >= len(i.pages) {
		return table.Row{}, table.ErrEOT
	}

	// no current page determined yet, choose the one under the currentPageIndex
	if i.currentPage == nil {
		p, err := tx.DataPage(i.table.name, i.pages[i.currentPageIndex])
		if err != nil {
			return table.Row{}, fmt.Errorf("load page: %w", err)
		}
		i.currentPage = p
	}

	// no slots determined yet, use the slots from the current page
	if i.slots == nil {
		i.slots = i.currentPage.OccupiedSlots()
	}
	// if the current slot is out of the bounds of the current slots, that means we are done
	// with this page and we should continue with the slots from the next page
	if i.currentSlot >= len(i.slots) {
		i.slots = nil
		i.currentSlot = 0
		i.currentPage = nil
		i.currentPageIndex++
		// we reset everything and increased the currentPageIndex, retry all of the above with the new
		// page index
		goto start
	}

	// get the record and deserialize it
	cell := i.currentPage.CellAt(i.slots[i.currentSlot]).(page.RecordCell)
	i.currentSlot++
	row, err := deserializeRow(i.cols, cell.Record)
	if err != nil {
		return table.Row{}, fmt.Errorf("deserialize: %w", err)
	}
	return row, nil
}

// Reset makes this iterator start over from the first row.
func (i *tableRowIterator) Reset() error {
	i.currentPageIndex = 0
	i.currentSlot = 0
	return nil
}

func (i *tableRowIterator) Close() error {
	return nil
}
