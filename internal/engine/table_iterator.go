package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/engine/page"

	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/schema"
	"github.com/xqueries/xdb/internal/engine/table"
)

type tableRowIterator struct {
	profiler *profile.Profiler

	schema *schema.Schema
	data   *dbfs.PagedFile

	pages            []page.ID
	currentPageIndex int
	currentPage      *page.Page

	slots       []page.Slot
	currentSlot int
}

func newTableRowIterator(profiler *profile.Profiler, schema *schema.Schema, data *dbfs.PagedFile) *tableRowIterator {
	return &tableRowIterator{
		profiler: profiler,
		schema:   schema,
		data:     data,
		pages:    data.Pages(),
	}
}

// Next returns the next row of this table iterator.
func (i *tableRowIterator) Next() (table.Row, error) {
	i.profiler.Enter("next row").Exit()

	if len(i.pages) == 0 {
		return table.Row{}, table.ErrEOT
	}

start:
	if i.currentPageIndex >= len(i.pages) {
		return table.Row{}, table.ErrEOT
	}

	if i.currentPage == nil {
		p, err := i.data.LoadPage(i.pages[i.currentPageIndex])
		if err != nil {
			return table.Row{}, fmt.Errorf("load page: %w", err)
		}
		i.currentPage = p
	}

	if i.slots == nil {
		i.slots = i.currentPage.OccupiedSlots()
	}
	if i.currentSlot >= len(i.slots) {
		i.currentSlot = 0
		i.currentPage = nil
		i.currentPageIndex++
		goto start
	}

	cell := i.currentPage.CellAt(i.slots[i.currentSlot]).(page.RecordCell)
	i.currentSlot++
	row, err := deserializeRow(i.schema.Cols(), cell.Record)
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
	return i.data.Close()
}
