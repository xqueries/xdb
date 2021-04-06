package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/schema"
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

	name         string
	schema       *schema.Schema
	dataProvider func() (*dbfs.PagedFile, error)
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

	schemaFile, err := tableMeta.SchemaFile()
	if err != nil {
		return nil, fmt.Errorf("obtain schema file: %w", err)
	}
	defer func() {
		_ = schemaFile.Close()
	}()

	var sch schema.Schema
	_, err = sch.ReadFrom(schemaFile)
	if err != nil {
		return nil, fmt.Errorf("load schema: %w", err)
	}

	return &Table{
		profiler:     e.profiler,
		dataProvider: tableMeta.DataFile,
		name:         name,
		schema:       &sch,
	}, nil
}

// Name returns the name of this table.
func (t Table) Name() string {
	return t.name
}

// Cols returns the column information of this table as a slice of projectedColumns.
func (t Table) Cols() []table.Col {
	return t.schema.Cols()
}

// Rows returns a row iterator that will lazily load rows from secondary
// storage. PLEASE NOTE THAT THE CALLER IS RESPONSIBLE FOR CLOSING THE
// OBTAINED ITERATOR, SO IT DOESN'T LEAK MEMORY.
func (t Table) Rows() (table.RowIterator, error) {
	data, err := t.dataProvider()
	if err != nil {
		return nil, fmt.Errorf("obtain data: %w", err)
	}
	return newTableRowIterator(t.profiler, t.schema, data), nil
}

// Insert inserts the given row into this table in secondary storage.
// This is done by simply adding a cell with the serialized row to the
// data page of this table.
func (t *Table) Insert(row table.Row) error {
	return ErrUnsupported
	// dataPage, err := t.dataPage.Load()
	// if err != nil {
	// 	return fmt.Errorf("load data page: %w", err)
	// }
	// defer t.dataPage.Unload()
	//
	// serializedRow, err := serializeRow(row)
	// if err != nil {
	// 	return fmt.Errorf("serialize row: %w", err)
	// }
	//
	// key := make([]byte, 4)
	// byteOrder.PutUint32(key, t.highestRowID)
	// t.highestRowID++
	// if err := dataPage.StoreRecordCell(page.RecordCell{
	// 	Key:    key,
	// 	Record: serializedRow,
	// }); err != nil {
	// 	return fmt.Errorf("store record cell: %w", err)
	// }
	// return nil
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

	// create schema
	var cols []table.Col
	for _, def := range cmd.ColumnDefs {
		cols = append(cols, table.Col{
			QualifiedName: def.Name,
			Type:          def.Type,
		})
	}
	newSchema := schema.New(cols)

	// store schema
	schemaFile, err := tableMeta.SchemaFile()
	if err != nil {
		return nil, fmt.Errorf("obtain schema: %w", err)
	}
	_, err = newSchema.WriteTo(schemaFile)
	if err != nil {
		return nil, fmt.Errorf("write schema: %w", err)
	}

	return table.Empty, nil
}

func frame(data []byte) []byte {
	buf := make([]byte, 4+len(data))
	byteOrder.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	return buf
}
