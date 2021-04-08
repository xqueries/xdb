package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/engine/page"
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/table"
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

	name string

	openSchemaFile func() (*dbfs.SchemaFile, error)
	openDataFile   func() (*dbfs.PagedFile, error)
}

// LoadTable loads a table with the given name from secondary storage. Only table meta
// information, such as column information are loaded into memory. No rows are read from
// disk by calling this method. To check whether a table with a given name exists in the
// currently loaded database, call HasTable.
func (e Engine) LoadTable(name string) (table.Table, error) {
	e.profiler.Enter("load table").Exit()

	tableMeta, err := e.dbfs.Table(name)
	if err != nil {
		return nil, fmt.Errorf("table: %w", err)
	}

	return &Table{
		profiler:       e.profiler,
		openSchemaFile: tableMeta.SchemaFile,
		openDataFile:   tableMeta.DataFile,
		name:           name,
	}, nil
}

// Name returns the name of this table.
func (t Table) Name() string {
	return t.name
}

// Cols returns the column information of this table as a slice of projectedColumns.
func (t Table) Cols() []table.Col {
	schemaFile, err := t.openSchemaFile()
	if err != nil {
		return nil
	}
	defer func() {
		_ = schemaFile.Close()
	}()
	return schemaFile.Columns()
}

// Rows returns a row iterator that will lazily load rows from secondary
// storage. PLEASE NOTE THAT THE CALLER IS RESPONSIBLE FOR CLOSING THE
// OBTAINED ITERATOR, SO IT DOESN'T LEAK MEMORY.
func (t Table) Rows() (table.RowIterator, error) {
	data, err := t.openDataFile()
	if err != nil {
		return nil, fmt.Errorf("open data file: %w", err)
	}
	return newTableRowIterator(t.profiler, t.Cols(), data), nil
}

// Insert inserts the given row into this table in secondary storage.
func (t *Table) Insert(row table.Row) error {
	dataFile, err := t.openDataFile()
	if err != nil {
		return fmt.Errorf("open data file: %w", err)
	}
	defer func() {
		_ = dataFile.Close()
	}()

	schemaFile, err := t.openSchemaFile()
	if err != nil {
		return fmt.Errorf("open schema file: %w", err)
	}
	defer func() {
		_ = schemaFile.Close()
	}()

	var p *page.Page
	if dataFile.PageCount() == 0 {
		// there are no pages in the data file yet, allocate one
		p, err = dataFile.AllocateNewPage()
		if err != nil {
			return fmt.Errorf("allocate first page: %w", err)
		}
	} else {
		// load last page in the data file
		highestPageID := dataFile.HighestPageID()
		p, err = dataFile.LoadPage(highestPageID)
		if err != nil {
			return fmt.Errorf("load page %v: %w", highestPageID, err)
		}
	}

	serializedRow, err := serializeRow(row)
	if err != nil {
		return fmt.Errorf("serialize row: %w", err)
	}

	key := make([]byte, 4)
	byteOrder.PutUint32(key, uint32(schemaFile.HighestRowID()))
	schemaFile.IncrementHighestRowID()
	if err := schemaFile.Store(); err != nil {
		return fmt.Errorf("store schema: %w", err)
	}

store:
	if err := p.StoreRecordCell(page.RecordCell{
		Key:    key,
		Record: serializedRow,
	}); err != nil {
		if err == page.ErrPageFull {
			p, err = dataFile.AllocateNewPage()
			if err != nil {
				return fmt.Errorf("allocate new page: %w", err)
			}
			goto store
		}
		return fmt.Errorf("store record cell: %w", err)
	}

	if err := dataFile.StorePage(p); err != nil {
		return fmt.Errorf("store page %v: %w", p.ID(), err)
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

	// create table files
	tableMeta, err := e.dbfs.CreateTable(cmd.Name)
	if err != nil {
		return nil, fmt.Errorf("create table files: %w", err)
	}

	schemaFile, err := tableMeta.SchemaFile()
	if err != nil {
		return nil, fmt.Errorf("load schema: %w", err)
	}

	// create schema
	var cols []table.Col
	for _, def := range cmd.ColumnDefs {
		cols = append(cols, table.Col{
			QualifiedName: def.Name,
			Type:          def.Type,
		})
	}
	schemaFile.SetColumns(cols)
	if err := schemaFile.Store(); err != nil {
		return nil, fmt.Errorf("store schema: %w", err)
	}

	return table.Empty, nil
}

func frame(data []byte) []byte {
	buf := make([]byte, 4+len(data))
	byteOrder.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	return buf
}
