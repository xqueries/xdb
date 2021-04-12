package transaction

import (
	"fmt"
	"sort"

	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/engine/page"
	"github.com/xqueries/xdb/internal/id"
)

type pageref struct {
	id    page.ID
	table string
}

// secondaryStorage describes a component of a transaction,
// that is used to load structures from files.
// This intermediate layer is used so that a transaction has
// no possibility to write anything back to disk.
type secondaryStorage interface {
	// loadTablesInfo loads the content of the tables.info file.
	loadTablesInfo() (*dbfs.TablesInfo, error)
	// loadSchemaFile loads the content of the schema file
	// of the table with the given name.
	loadSchemaFile(string) (*dbfs.SchemaFile, error)
	// loadDataPage loads the page with the given ID from the
	// data file of the table with the given name.
	loadDataPage(string, page.ID) (*page.Page, error)

	// unusedPageID returns a currently unused page ID that can be
	// used to allocate pages within a transaction. The ID
	// is exclusive to this transaction.
	unusedPageID(string) (page.ID, error)
	// availableDataPages returns a slice of all existing available
	// pages of a data file of the table with the given name.
	availableDataPages(string) ([]page.ID, error)
	hasTable(string) (bool, error)
}

// TX represents a single transaction or unit of work
// in the database. A transaction can load and modify pages.
// Changes across multiple transactions will be merged by the
// transaction manager upon commit.
type TX struct {
	ID id.ID

	secondaryStorage secondaryStorage

	// state is the current state of this transaction.
	// A newly created transaction always is StatePending.
	state State

	// createdTables is a string slice containing all table names
	// that were created in this transaction.
	// Schemas and data pages of this transaction may reference
	// tables that do not exist on disk yet, however, these tables
	// will be created (on disk), if they are listed in this slice.
	createdTables []string

	newlyAllocatedPages map[string][]*page.Page
	// tableSchemas associates a table name with the schema file
	// (of that table). If modified, the transaction manager will persist
	// the changes upon transaction commit.
	tableSchemas map[string]*dbfs.SchemaFile
	// dataPages are potentially modified pages, that the transaction
	// manager has to persist onto disk upon transaction commit.
	dataPages map[pageref]*page.Page
}

func newTransaction(secondaryStorage secondaryStorage) *TX {
	return &TX{
		ID:                  id.Create(),
		secondaryStorage:    secondaryStorage,
		state:               StatePending,
		newlyAllocatedPages: make(map[string][]*page.Page),
		tableSchemas:        make(map[string]*dbfs.SchemaFile),
		dataPages:           make(map[pageref]*page.Page),
	}
}

func (tx TX) State() State {
	return tx.state
}

func (tx *TX) DataPage(table string, id page.ID) (*page.Page, error) {
	pr := pageref{id, table}
	if cached, ok := tx.dataPages[pr]; ok {
		return cached, nil
	}

	if newlyAllocatedPages, ok := tx.newlyAllocatedPages[table]; ok {
		for _, newlyAllocatedPage := range newlyAllocatedPages {
			if newlyAllocatedPage.ID() == id {
				return newlyAllocatedPage, nil
			}
		}
	}

	p, err := tx.secondaryStorage.loadDataPage(table, id)
	if err != nil {
		return nil, fmt.Errorf("load data page from disk: %w", err)
	}
	tx.dataPages[pr] = p
	return p, nil
}

// DataPageReadOnly will load the requested page from the given table in read-only
// mode, meaning that you can modify the page, but all modifications will be lost
// as soon as the GC collects the page. If the requested page already exists in memory,
// a copy of the cached page will be returned. If not, the requested page will be loaded
// from secondary storage, but will not be cached.
func (tx *TX) DataPageReadOnly(table string, id page.ID) (*page.Page, error) {
	pr := pageref{id, table}
	if cached, ok := tx.dataPages[pr]; ok {
		p, err := page.Load(cached.CopyOfData())
		if err != nil {
			return nil, fmt.Errorf("load: %w", err)
		}
		return p, nil
	}

	if newlyAllocatedPages, ok := tx.newlyAllocatedPages[table]; ok {
		for _, newlyAllocatedPage := range newlyAllocatedPages {
			if newlyAllocatedPage.ID() == id {
				return newlyAllocatedPage, nil
			}
		}
	}

	p, err := tx.secondaryStorage.loadDataPage(table, id)
	if err != nil {
		return nil, fmt.Errorf("load data page from disk: %w", err)
	}
	return p, nil
}

func (tx *TX) SchemaFile(table string) (*dbfs.SchemaFile, error) {
	if cached, ok := tx.tableSchemas[table]; ok {
		return cached, nil
	}

	if tx.tableWasCreatedInThisTransaction(table) {
		var sf dbfs.SchemaFile
		tx.tableSchemas[table] = &sf
		return &sf, nil
	}

	info, err := tx.secondaryStorage.loadSchemaFile(table)
	if err != nil {
		return nil, fmt.Errorf("load schema from disk: %w", err)
	}
	tx.tableSchemas[table] = info
	return info, nil
}

func (tx *TX) tableWasCreatedInThisTransaction(name string) bool {
	index := sort.SearchStrings(tx.createdTables, name)
	return index < len(tx.createdTables) && tx.createdTables[index] == name
}

func (tx *TX) HasTable(name string) (bool, error) {
	if tx.tableWasCreatedInThisTransaction(name) {
		return true, nil
	}

	return tx.secondaryStorage.hasTable(name)
}

func (tx *TX) CreateTable(name string) error {
	if ok, err := tx.HasTable(name); ok {
		return fmt.Errorf("table already exists in this transaction")
	} else if err != nil {
		return fmt.Errorf("has table: %w", err)
	}

	insertIndex := sort.SearchStrings(tx.createdTables, name)
	tx.createdTables = append(tx.createdTables[:insertIndex], append([]string{name}, tx.createdTables[insertIndex:]...)...)

	tx.tableSchemas[name] = &dbfs.SchemaFile{}

	return nil
}

func (tx *TX) AllocateNewDataPage(table string) (*page.Page, error) {
	var newID page.ID

	if tx.tableWasCreatedInThisTransaction(table) {
	outer:
		for _, p := range tx.newlyAllocatedPages[table] {
			for newID = page.ID(0); ; newID++ {
				if newID != p.ID() {
					break outer
				}
			}
		}
	} else {
		var err error
		newID, err = tx.secondaryStorage.unusedPageID(table)
		if err != nil {
			return nil, fmt.Errorf("unused page ID: %w", err)
		}
	}

	newPage, err := page.New(newID)
	if err != nil {
		return nil, fmt.Errorf("new: %w", err)
	}
	tx.newlyAllocatedPages[table] = append(tx.newlyAllocatedPages[table], newPage)
	return newPage, nil
}

func (tx *TX) ExistingDataPagesForTable(table string) ([]page.ID, error) {
	var ids []page.ID
	for _, p := range tx.newlyAllocatedPages[table] {
		ids = append(ids, p.ID())
	}
	if tx.tableWasCreatedInThisTransaction(table) {
		return ids, nil
	}

	diskPages, err := tx.secondaryStorage.availableDataPages(table)
	if err != nil {
		return nil, fmt.Errorf("available data pages: %w", err)
	}
	ids = append(ids, diskPages...)
	return ids, nil
}
