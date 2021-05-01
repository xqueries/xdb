package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/page"
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/transaction"
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
	// tx is the transaction that is accessed by this table.
	// This means, that data for reads must originate from this
	// transaction, and writes must be performed into this transaction.
	tx *transaction.TX

	name string
}

// LoadTable loads a table with the given name from secondary storage. Only table meta
// information, such as column information are loaded into memory. No rows are read from
// disk by calling this method. To check whether a table with a given name exists in the
// currently loaded database, call HasTable.
func (e Engine) LoadTable(tx *transaction.TX, name string) (table.Table, error) {
	e.profiler.Enter("load table").Exit()

	if ok, err := tx.HasTable(name); !ok {
		return nil, fmt.Errorf("table '%s' does not exist", name)
	} else if err != nil {
		return nil, fmt.Errorf("has table: %w", err)
	}

	return &Table{
		profiler: e.profiler,
		tx:       tx,
		name:     name,
	}, nil
}

// Name returns the name of this table.
func (t Table) Name() string {
	return t.name
}

// Cols returns the column information of this table as a slice of projectedColumns.
func (t *Table) Cols() ([]table.Col, error) {
	sf, err := t.tx.SchemaFile(t.name)
	if err != nil {
		return nil, fmt.Errorf("schema file: %w", err)
	}
	return sf.Columns, nil
}

// Rows returns a row iterator that will iterate over all rows in this table.
// Data is read from the transaction that this table belongs to.
func (t *Table) Rows() (table.RowIterator, error) {
	it, err := newTableRowIterator(t)
	if err != nil {
		return nil, fmt.Errorf("new table row iterator: %w", err)
	}
	return it, nil
}

// Insert inserts the given row into this table in secondary storage.
func (t *Table) Insert(row table.Row) error {
	tx := t.tx

	schemaFile, err := tx.SchemaFile(t.name)
	if err != nil {
		return fmt.Errorf("schema file: %w", err)
	}

	serializedRow, err := serializeRow(row)
	if err != nil {
		return fmt.Errorf("serialize row: %w", err)
	}

	key := make([]byte, 4)
	byteOrder.PutUint32(key, uint32(schemaFile.HighestRowID))
	record := page.RecordCell{
		Key:    key,
		Record: serializedRow,
	}

	// find a page to insert the row to
	availablePageIDs, err := tx.ExistingDataPagesForTable(t.name)
	if err != nil {
		return fmt.Errorf("existing data pages: %w", err)
	}

	var found bool
	var foundID page.ID
	for _, pageID := range availablePageIDs {
		roPage, err := tx.DataPageReadOnly(t.name, pageID)
		if err != nil {
			return fmt.Errorf("data page read-only: %w", err)
		}
		if roPage.CanAccommodateRecord(record) {
			found = true
			foundID = pageID
			break
		}
	}

	var p *page.Page
	if !found {
		alloc, err := tx.AllocateNewDataPage(t.name)
		if err != nil {
			return fmt.Errorf("allocate new page: %w", err)
		}
		p = alloc
	} else {
		loaded, err := tx.DataPage(t.name, foundID)
		if err != nil {
			return fmt.Errorf("data page: %w", err)
		}
		p = loaded
	}

	// only increment highest row ID if cell was actually inserted
	schemaFile.HighestRowID++

	if err := p.StoreRecordCell(record); err != nil {
		// cannot be ErrPageFull because we checked whether or not the page
		// can accommodate the record that we want to store
		return fmt.Errorf("store record cell: %w", err)
	}

	return nil
}

// evaluateCreateTable creates a new table from the given command.
func (e Engine) evaluateCreateTable(ctx ExecutionContext, cmd command.CreateTable) (table.Table, error) {
	defer e.profiler.Enter("create table").Exit()
	tx := ctx.tx

	/*
		Since we don't support table deletion yet, we can't support overwriting tables.
		However, the behavior below, that checks if the table already exists, is correct.
	*/
	if cmd.Overwrite {
		return nil, fmt.Errorf("overwrite: %w", ErrUnsupported)
	}

	if !cmd.Overwrite {
		if ok, err := tx.HasTable(cmd.Name); err != nil {
			return nil, fmt.Errorf("has table: %w", err)
		} else if ok {
			return nil, fmt.Errorf("%v: %w", cmd.Name, ErrAlreadyExists)
		}
	}

	if err := tx.CreateTable(cmd.Name); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	sf, err := tx.SchemaFile(cmd.Name)
	if err != nil {
		return nil, fmt.Errorf("schema file: %w", err)
	}

	// create schema
	var cols []table.Col
	for _, def := range cmd.ColumnDefs {
		cols = append(cols, table.Col{
			QualifiedName: def.Name,
			Type:          def.Type,
		})
	}
	sf.Columns = cols

	return table.Empty, nil
}

func frame(data []byte) []byte {
	buf := make([]byte, 4+len(data))
	byteOrder.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	return buf
}
