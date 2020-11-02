package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/engine/storage/page"
)

// Info holds information about different components of the opened database.
type (
	Info struct {
		tables map[string]TableInfo
	}

	TableInfo struct {
		// Page is the ID of the table page of this table.
		Page page.ID
		// RowAmount is the amount of rows that this table has.
		RowAmount int64
		// Name is the name of this table, including the schema.
		Name string
	}
)

// Info computes an information summary about the data within this database.
// That summary contains a list of tables.
func (e Engine) Info() (Info, error) {
	info := Info{
		tables: make(map[string]TableInfo),
	}

	tablesPage, err := e.tablesPageContainer.Load()
	if err != nil {
		return Info{}, fmt.Errorf("load tables page: %w", err)
	}
	defer e.tablesPageContainer.Unload()

	tableCells := tablesPage.Cells()

	for _, cell := range tableCells {
		if cell.Type() != page.CellTypePointer {
			// only consider pointer cells
			continue
		}
		pointerCell := cell.(page.PointerCell)
		tableName := string(pointerCell.Key)
		tableInfo, err := e.infoForTable(tableName, pointerCell.Pointer)
		if err != nil {
			return Info{}, fmt.Errorf("info for table '%s': %w", tableName, err)
		}
		info.tables[tableName] = tableInfo
	}

	return info, nil
}

// TableNames returns a string slice containing all table names that are currently available in the
// database.
func (i Info) TableNames() []string {
	var names []string
	for name := range i.tables {
		names = append(names, name)
	}
	return names
}

// Tables returns a slice of TableInfo, which holds information about every table that is currently
// available in the database.
func (i Info) Tables() []TableInfo {
	var tbls []TableInfo
	for _, infoTable := range i.tables {
		tbls = append(tbls, infoTable)
	}
	return tbls
}

// Table returns a TableInfo, which holds information about the table with the given name.
// This method returns false, if no table with the given name is currently available in the database.
func (i Info) Table(name string) (TableInfo, bool) {
	infoTable, ok := i.tables[name]
	return infoTable, ok
}

func (e Engine) infoForTable(tableName string, tablePageID page.ID) (TableInfo, error) {
	// the row amount is just the amount of cells in the data page
	tablePageContainer := e.NewPageContainer(tablePageID)
	tablePage, err := tablePageContainer.Load()
	if err != nil {
		return TableInfo{}, fmt.Errorf("load table page: %w", err)
	}
	defer tablePageContainer.Unload()

	dataCell, ok := tablePage.CellByString(TableKeyData)
	if !ok || dataCell.Type() != page.CellTypePointer {
		return TableInfo{}, fmt.Errorf("no data cell on table page %v", tableName)
	}

	dataPageContainer := e.NewPageContainer(dataCell.(page.PointerCell).Pointer)
	dataPage, err := dataPageContainer.Load()
	if err != nil {
		return TableInfo{}, fmt.Errorf("load data page: %w", err)
	}
	defer dataPageContainer.Unload()

	// we interpret the amount of record cells on the data page as the amount of rows
	recordCellAmount := int64(0)
	cells := dataPage.Cells()
	// only consider record cells
	for _, cell := range cells {
		if cell.Type() == page.CellTypeRecord {
			recordCellAmount++
		}
	}

	return TableInfo{
		Name:      tableName,
		Page:      tablePageID,
		RowAmount: recordCellAmount,
	}, nil
}
