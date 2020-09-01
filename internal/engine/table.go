package engine

import (
	"bytes"
	"fmt"
	"io"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/storage/page"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

// Constants for cells that are required in every table page.
// Obtain the value with CellByString(theConstant).
const (
	TableKeyName         = "name"
	TableKeyData         = "data"
	TableKeyColInfo      = "colinfo"
	TableKeyHighestRowID = "highestROWID"
)

var _ Namer = (*Table)(nil)
var _ Inserter = (*Table)(nil)

// Namer wraps the simple method Name, which returns the name of the
// implementing component. Tables may implement this interface, if they
// are named.
type Namer interface {
	Name() string
}

// Inserter describes a component in which a row can be inserted.
// Tables may implement this interface, if they allow insertion.
type Inserter interface {
	Insert(table.Row) error
}

// Table is a representation of an on-disk table used by the engine.
// It is an intermediate layer to access and manipulate data inside
// the table's pages.
type Table struct {
	profiler *profile.Profiler

	// storage-facing fields

	tablePage PageContainer
	dataPage  PageContainer

	// application-facing fields

	name         string
	highestRowID uint32
	cols         []table.Col
}

// LoadTable loads a table with the given name from secondary storage. Only table meta
// information, such as column information are loaded into memory. No rows are read from
// disk by calling this method. To check whether a table with a given name exists in the
// currently loaded database, call HasTable.
func (e Engine) LoadTable(name string) (table.Table, error) {
	e.profiler.Enter("load table").Exit()

	tablesPage, err := e.tablesPageContainer.Load()
	if err != nil {
		return nil, fmt.Errorf("load tables page: %w", err)
	}
	defer e.tablesPageContainer.Unload()

	tablePageIDCell, ok := tablesPage.CellByString(name)
	if !ok || tablePageIDCell.Type() != page.CellTypePointer {
		return nil, fmt.Errorf("no table with name '%v'", name)
	}
	tablePageID := tablePageIDCell.(page.PointerCell).Pointer
	tablePageContainer := e.NewPageContainer(tablePageID)

	tablePage, err := tablePageContainer.Load()
	if err != nil {
		return nil, fmt.Errorf("load table page: %w", err)
	}
	defer tablePageContainer.Unload()

	nameCell, ok := tablePage.CellByString(TableKeyName)
	if !ok || nameCell.Type() != page.CellTypeRecord {
		return nil, fmt.Errorf("no name cell on table page")
	}
	tableName := string(nameCell.(page.RecordCell).Record)

	dataCell, ok := tablePage.CellByString(TableKeyData)
	if !ok || dataCell.Type() != page.CellTypePointer {
		return nil, fmt.Errorf("no data cell on table page %v", tableName)
	}

	rowIDCell, ok := tablePage.CellByString(TableKeyHighestRowID)
	if !ok || rowIDCell.Type() != page.CellTypeRecord {
		return nil, fmt.Errorf("no highest row ID cell on table page %v", tableName)
	}

	colInfoCell, ok := tablePage.CellByString(TableKeyColInfo)
	if !ok || colInfoCell.Type() != page.CellTypeRecord {
		return nil, fmt.Errorf("no col info cell on table page %v", tableName)
	}
	record := colInfoCell.(page.RecordCell).Record
	cols, err := deserializeColInfo(record)
	if err != nil {
		return nil, err
	}

	return &Table{
		profiler:     e.profiler,
		tablePage:    tablePageContainer,
		dataPage:     e.NewPageContainer(dataCell.(page.PointerCell).Pointer),
		name:         tableName,
		cols:         cols,
		highestRowID: byteOrder.Uint32(rowIDCell.(page.RecordCell).Record),
	}, nil
}

// Name returns the name of this table.
func (t Table) Name() string {
	return t.name
}

// Cols returns the column information of this table as a slice of columns.
func (t Table) Cols() []table.Col {
	return t.cols
}

// Rows returns a row iterator that will lazily load rows from secondary
// storage. PLEASE NOTE THAT THE CALLER IS RESPONSIBILE FOR EXHAUSTING THE
// OBTAINED ITERATOR, SO IT DOESN'T LEAK MEMORY.
func (t Table) Rows() (table.RowIterator, error) {
	dataPage, err := t.dataPage.Load()
	if err != nil {
		return nil, fmt.Errorf("load data page: %w", err)
	}
	defer t.dataPage.Unload()

	return newTableRowIterator(t.profiler, t.cols, dataPage), nil
}

// Insert inserts the given row into this table in secondary storage.
// This is done by simply adding a cell with the serialized row to the
// data page of this table.
func (t *Table) Insert(row table.Row) error {
	dataPage, err := t.dataPage.Load()
	if err != nil {
		return fmt.Errorf("load data page: %w", err)
	}
	defer t.dataPage.Unload()

	serializedRow, err := serializeRow(row)
	if err != nil {
		return fmt.Errorf("serialize row: %w", err)
	}

	key := make([]byte, 4)
	byteOrder.PutUint32(key, t.highestRowID)
	t.highestRowID++
	if err := dataPage.StoreRecordCell(page.RecordCell{
		Key:    key,
		Record: serializedRow,
	}); err != nil {
		return fmt.Errorf("store record cell: %w", err)
	}
	return nil
}

// evaluateCreateTable creates a new table from the given command.
func (e Engine) evaluateCreateTable(_ ExecutionContext, cmd command.CreateTable) (table.Table, error) {
	defer e.profiler.Enter("create table").Exit()

	/*
		Since we don't support table deletion yet, we can't support overwriting tables.
		However, the behavior below, that checks if the table already exists, is correct.
	*/
	if cmd.Overwrite {
		return nil, fmt.Errorf("overwrite: %w", ErrUnsupported)
	}

	if !cmd.Overwrite && e.HasTable(cmd.Name) {
		return nil, fmt.Errorf("%v: %w", cmd.Name, ErrAlreadyExists)
	}

	// allocate a new page for the new table
	newTablePageID, err := e.dbFile.AllocateNewPage()
	if err != nil {
		return nil, fmt.Errorf("allocate new page: %w", err)
	}

	newDataPageID, err := e.dbFile.AllocateNewPage()
	if err != nil {
		return nil, fmt.Errorf("allocate new page: %w", err)
	}

	// store required cells on table page
	tablePage, err := e.pageCache.FetchAndPin(newTablePageID)
	if err != nil {
		return nil, fmt.Errorf("fetch and pin table page: %w", err)
	}
	defer e.pageCache.Unpin(newTablePageID)
	// name cell
	if err := tablePage.StoreRecordCell(page.RecordCell{
		Key:    []byte(TableKeyName),
		Record: []byte(cmd.Name),
	}); err != nil {
		return nil, fmt.Errorf("store %v: %w", TableKeyName, err)
	}
	// data cell
	if err := tablePage.StorePointerCell(page.PointerCell{
		Key:     []byte(TableKeyData),
		Pointer: newDataPageID,
	}); err != nil {
		return nil, fmt.Errorf("store %v: %w", TableKeyData, err)
	}
	// highestRowID cell
	if err := tablePage.StoreRecordCell(page.RecordCell{
		Key:    []byte(TableKeyHighestRowID),
		Record: []byte{0, 0, 0, 0},
	}); err != nil {
		return nil, fmt.Errorf("store %v: %w", TableKeyHighestRowID, err)
	}
	// colinfo cell
	var cols []table.Col
	for _, def := range cmd.ColumnDefs {
		cols = append(cols, table.Col{
			QualifiedName: def.Name,
			Type:          def.Type,
		})
	}
	serializedInfo, err := serializeColInfo(cols)
	if err != nil {
		return nil, fmt.Errorf("serialize col info: %w", err)
	}
	if err := tablePage.StoreRecordCell(page.RecordCell{
		Key:    []byte(TableKeyColInfo),
		Record: serializedInfo,
	}); err != nil {
		return nil, fmt.Errorf("store %v: %w", TableKeyColInfo, err)
	}

	// obtain the tables page
	tablesPage, err := e.tablesPageContainer.Load()
	if err != nil {
		return nil, fmt.Errorf("load tables page: %w", err)
	}
	defer e.tablesPageContainer.Unload()

	// register the new table in the tables page
	if err := tablesPage.StorePointerCell(page.PointerCell{
		Key:     []byte(cmd.Name),
		Pointer: newTablePageID,
	}); err != nil {
		return nil, fmt.Errorf("store pointer cell to %v: %w", newTablePageID, err)
	}

	return table.Empty, nil
}

func serializeColInfo(cols []table.Col) ([]byte, error) {
	var buf bytes.Buffer

	for _, col := range cols {
		typeIndicator := types.IndicatorFor(col.Type)
		if typeIndicator == types.TypeIndicatorUnknown {
			return nil, fmt.Errorf("unknown type indicator for type %v", col.Type)
		}
		_ = buf.WriteByte(byte(typeIndicator))
		_, _ = buf.Write(frame([]byte(col.QualifiedName)))
	}

	return buf.Bytes(), nil
}

func deserializeColInfo(record []byte) (cols []table.Col, err error) {
	colInfo := bytes.NewBuffer(record)
	for {
		// type indicator
		typeIndicator, err := colInfo.ReadByte()
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read type indicator: %w", err)
		} else if err == io.EOF {
			break
		}
		// col name frame
		frame := make([]byte, 4)
		n, err := colInfo.Read(frame)
		if err != nil {
			return nil, fmt.Errorf("read frame: %w", err)
		}
		if n != 4 {
			return nil, fmt.Errorf("read frame: expected %v bytes, could only read %v", 4, n)
		}
		// col name
		buf := make([]byte, byteOrder.Uint32(frame))
		n, err = colInfo.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("read col name: %w", err)
		}
		if n != len(buf) {
			return nil, fmt.Errorf("read col name: expected %v bytes, could only read %v", len(buf), n)
		}
		// col read successfully, append to cols
		cols = append(cols, table.Col{
			QualifiedName: string(buf),
			Type:          types.ByIndicator(types.TypeIndicator(typeIndicator)),
		})
	}
	return cols, nil
}

func frame(data []byte) []byte {
	buf := make([]byte, 4+len(data))
	byteOrder.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	return buf
}
