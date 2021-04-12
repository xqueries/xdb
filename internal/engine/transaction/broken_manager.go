package transaction

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/engine/page"
	"github.com/xqueries/xdb/internal/id"
)

var _ Manager = (*brokenManager)(nil)

type brokenManager struct {
	log                 zerolog.Logger
	dbfs                *dbfs.DBFS
	pendingTransactions map[id.ID]*TX
}

// NewBrokenManager returns a transaction manager that probably works, but it's just as likely
// that it is broken - as the name says.
func NewBrokenManager(log zerolog.Logger, dbfs *dbfs.DBFS) Manager {
	return &brokenManager{
		log:                 log,
		dbfs:                dbfs,
		pendingTransactions: make(map[id.ID]*TX),
	}
}

func (m *brokenManager) Close() error {
	return m.dbfs.Close()
}

func (m *brokenManager) Start() (*TX, error) {
	tx := newTransaction(m)
	m.pendingTransactions[tx.ID] = tx
	return tx, nil
}

func (m *brokenManager) Commit(tx *TX) error {
	if tx.state != StatePending {
		return fmt.Errorf("can only commit transactions with state %v, but got state %v", StatePending, tx.state)
	}

	m.log.Debug().
		Stringer("tx", tx.ID).
		Msg("commit transaction")

	// process all tables that must be created on disk
	{
		for _, tableName := range tx.createdTables {
			m.log.Trace().
				Stringer("tx", tx.ID).
				Str("table", tableName).
				Msg("create table on file system")
			if _, err := m.dbfs.CreateTable(tableName); err != nil {
				return fmt.Errorf("create table: %w", err)
			}
		}
	}
	// persist changes to table schemas
	{
		for tbl, schemaFile := range tx.tableSchemas {
			m.log.Trace().
				Stringer("tx", tx.ID).
				Str("table", tbl).
				Msg("persist table schema")
			if err := m.dbfs.StoreSchema(tbl, schemaFile); err != nil {
				return fmt.Errorf("store schema: %w", err)
			}
		}
	}
	// persist all pages
	{
		// open all data files
		dataPagedFiles := make(map[string]*dbfs.PagedFile)
		for ref := range tx.dataPages {
			if _, ok := dataPagedFiles[ref.table]; ok {
				continue
			}
			tbl, err := m.dbfs.Table(ref.table)
			if err != nil {
				return fmt.Errorf("table: %w", err)
			}
			dataFile, err := tbl.DataFile()
			if err != nil {
				return fmt.Errorf("data file: %w", err)
			}
			dataPagedFiles[ref.table] = dataFile
		}
		for table := range tx.newlyAllocatedPages {
			if _, ok := dataPagedFiles[table]; ok {
				continue
			}
			tbl, err := m.dbfs.Table(table)
			if err != nil {
				return fmt.Errorf("table: %w", err)
			}
			dataFile, err := tbl.DataFile()
			if err != nil {
				return fmt.Errorf("data file: %w", err)
			}
			dataPagedFiles[table] = dataFile
		}

		// all data files are open, we assume that we can write without problems
		for ref, p := range tx.dataPages { // probably more efficient in I/O when sorted by data file i.e. table
			m.log.Trace().
				Stringer("tx", tx.ID).
				Str("table", ref.table).
				Uint32("page", ref.id).
				Msg("write data page")
			if err := dataPagedFiles[ref.table].StorePage(p); err != nil {
				return fmt.Errorf("store page: %w", err) // this is a potential data integrity violation
			}
		}

		for table, pages := range tx.newlyAllocatedPages {
			for _, p := range pages {
				m.log.Trace().
					Stringer("tx", tx.ID).
					Str("table", table).
					Uint32("page", p.ID()).
					Msg("allocate new page")
				if _, err := dataPagedFiles[table].AllocatePageWithID(p.ID()); err != nil {
					return fmt.Errorf("allocate with ID: %w", err)
				}
				if err := dataPagedFiles[table].StorePage(p); err != nil {
					return fmt.Errorf("store page: %w", err)
				}
			}
		}

		// close all opened paged files
		for _, pf := range dataPagedFiles {
			if err := pf.Close(); err != nil {
				return fmt.Errorf("close paged file: %w", err)
			}
		}
	}

	tx.state = StateCommitted
	delete(m.pendingTransactions, tx.ID)

	return nil
}

func (m *brokenManager) Rollback(tx *TX) error {
	if tx.state != StatePending {
		return fmt.Errorf("can only rollback transactions with state %v, but got state %v", StatePending, tx.state)
	}

	tx.state = StateRolledBack
	delete(m.pendingTransactions, tx.ID)

	return nil
}

func (m *brokenManager) loadDataPage(table string, id page.ID) (*page.Page, error) {
	tbl, err := m.dbfs.Table(table)
	if err != nil {
		return nil, err
	}
	pf, err := tbl.DataFile()
	if err != nil {
		return nil, fmt.Errorf("data file: %w", err)
	}
	p, err := pf.LoadPage(id)
	if err != nil {
		return nil, fmt.Errorf("load page: %w", err)
	}
	return p, nil
}

func (m *brokenManager) unusedPageID(table string) (page.ID, error) {
	tbl, err := m.dbfs.Table(table)
	if err != nil {
		return 0, err
	}
	pf, err := tbl.DataFile()
	if err != nil {
		return 0, fmt.Errorf("data file: %w", err)
	}
	unusedPageID, err := pf.FindUnusedPageID()
	if err != nil {
		return 0, fmt.Errorf("find unused page ID: %w", err)
	}
	// FIXME: unusedPageID must be exclusive to a single transaction
	return unusedPageID, nil
}

func (m *brokenManager) loadSchemaFile(table string) (*dbfs.SchemaFile, error) {
	tbl, err := m.dbfs.Table(table)
	if err != nil {
		return nil, err
	}
	sf, err := tbl.SchemaFile()
	if err != nil {
		return nil, fmt.Errorf("load schema: %w", err)
	}
	return sf, nil
}

func (m *brokenManager) loadTablesInfo() (*dbfs.TablesInfo, error) {
	tablesInfo, err := m.dbfs.LoadTablesInfo()
	if err != nil {
		return nil, err
	}
	return &tablesInfo, nil
}

func (m *brokenManager) availableDataPages(table string) ([]page.ID, error) {
	tbl, err := m.dbfs.Table(table)
	if err != nil {
		return nil, err
	}
	pf, err := tbl.DataFile()
	if err != nil {
		return nil, fmt.Errorf("data file: %w", err)
	}
	return pf.Pages(), nil
}

func (m *brokenManager) hasTable(table string) (bool, error) {
	tablesInfo, err := m.dbfs.LoadTablesInfo()
	if err != nil {
		return false, fmt.Errorf("tables info: %w", err)
	}
	_, ok := tablesInfo.Tables[table]
	return ok, nil
}
