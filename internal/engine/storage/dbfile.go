package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/rs/zerolog"
	"github.com/spf13/afero"
	"github.com/xqueries/xdb/internal/engine/storage/cache"
	"github.com/xqueries/xdb/internal/engine/storage/page"
	"github.com/xqueries/xdb/internal/multierr"
)

const (
	// DefaultCacheSize is the default amount of pages, that the cache can hold
	// at most. Current limit is 256, which, regarding the current page size of
	// 16K, means, that the maximum size that a full cache will occupy, is 4M
	// (CacheSize * page.Size).
	DefaultCacheSize = 1 << 8

	// HeaderPageID is the page ID of the header page of the database file.
	HeaderPageID page.ID = 0

	// HeaderTables is the string key for the header page's cell "tables"
	HeaderTables = "tables"
	// HeaderConfig is the string key for the header page's cell "config"
	HeaderConfig = "config"
)

// DBFile is a database file that can be opened or created. From this file, you
// can obtain a page cache, which you must use for reading pages.
type DBFile struct {
	closed uint32

	log       zerolog.Logger
	cacheSize int

	file        afero.File
	pageManager *PageManager
	cache       cache.Cache

	headerPageID page.ID
	tablesPageID page.ID
	configPageID page.ID
}

// Create creates a new database in the given file with the given options. The
// file must exist, but be empty and must be a regular file.
func Create(file afero.File, opts ...Option) (*DBFile, error) {
	if info, err := file.Stat(); err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	} else if info.IsDir() {
		return nil, fmt.Errorf("file is directory")
	} else if size := info.Size(); size != 0 {
		return nil, fmt.Errorf("file is not empty, has %v bytes", size)
	} else if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("file is not a regular file")
	}

	mgr, err := NewPageManager(file)
	if err != nil {
		return nil, fmt.Errorf("new page manager: %w", err)
	}

	headerPage, err := mgr.AllocateNew()
	if err != nil {
		return nil, fmt.Errorf("allocate header page: %w", err)
	}
	configPage, err := mgr.AllocateNew()
	if err != nil {
		return nil, fmt.Errorf("allocate config page: %w", err)
	}
	tablesPage, err := mgr.AllocateNew()
	if err != nil {
		return nil, fmt.Errorf("allocate tables page: %w", err)
	}

	// store pointer to config page
	if err := headerPage.StorePointerCell(page.PointerCell{
		Key:     []byte(HeaderConfig),
		Pointer: configPage.ID(),
	}); err != nil {
		return nil, fmt.Errorf("store config pointer: %w", err)
	}
	// store pointer to tables page
	if err := headerPage.StorePointerCell(page.PointerCell{
		Key:     []byte(HeaderTables),
		Pointer: tablesPage.ID(),
	}); err != nil {
		return nil, fmt.Errorf("store tables pointer: %w", err)
	}

	err = mgr.WritePage(headerPage) // immediately flush
	if err != nil {
		return nil, fmt.Errorf("write header page: %w", err)
	}
	err = mgr.WritePage(configPage) // immediately flush
	if err != nil {
		return nil, fmt.Errorf("write config page: %w", err)
	}
	err = mgr.WritePage(tablesPage) // immediately flush
	if err != nil {
		return nil, fmt.Errorf("write tables page: %w", err)
	}

	return newDB(file, mgr, headerPage, tablesPage, configPage, opts...)
}

// Open opens and validates a given file and creates a (*DBFile) with the given
// options. If the validation fails, an error explaining the failure will be
// returned.
func Open(file afero.File, opts ...Option) (*DBFile, error) {
	if err := NewValidator(file).Validate(); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	mgr, err := NewPageManager(file)
	if err != nil {
		return nil, fmt.Errorf("new page manager: %w", err)
	}

	headerPage, err := mgr.ReadPage(HeaderPageID)
	if err != nil {
		return nil, fmt.Errorf("read header page: %w", err)
	}

	tablesPageID, err := pointerCellValue(headerPage, HeaderTables)
	if err != nil {
		return nil, fmt.Errorf("pointer cell value: %w", err)
	}

	tablesPage, err := mgr.ReadPage(tablesPageID)
	if err != nil {
		return nil, fmt.Errorf("read tables page: %w", err)
	}

	configPageID, err := pointerCellValue(headerPage, HeaderConfig)
	if err != nil {
		return nil, fmt.Errorf("pointer cell value: %w", err)
	}
	configPage, err := mgr.ReadPage(configPageID)
	if err != nil {
		return nil, fmt.Errorf("read config page: %w", err)
	}

	return newDB(file, mgr, headerPage, tablesPage, configPage, opts...)
}

// AllocateNewPage allocates and immediately persists a new page in secondary
// storage. This will fail if the DBFile is closed. After this method returns,
// the allocated page can immediately be found by the cache (it is not loaded
// yet), and you can use the returned page ID to load the page through the
// cache.
func (db *DBFile) AllocateNewPage() (page.ID, error) {
	if db.Closed() {
		return 0, ErrClosed
	}

	headerPage, err := db.pageManager.ReadPage(db.headerPageID)
	if err != nil {
		return 0, fmt.Errorf("read header page: %w", err)
	}

	page, err := db.pageManager.AllocateNew()
	if err != nil {
		return 0, fmt.Errorf("allocate new: %w", err)
	}
	if err := db.pageManager.WritePage(headerPage); err != nil {
		return 0, fmt.Errorf("write header page: %w", err)
	}
	return page.ID(), nil
}

// Cache returns the cache implementation, that you must use to obtain pages.
// This will fail if the DBFile is closed.
func (db *DBFile) Cache() cache.Cache {
	if db.Closed() {
		return nil
	}
	return db.cache
}

// TablesPageID returns the ID of the tables page in this database file.
func (db *DBFile) TablesPageID() page.ID {
	return db.tablesPageID
}

// Close will close the underlying cache, as well as page manager, as well as
// the file. Everything will be closed after writing the config and header page.
func (db *DBFile) Close() error {
	errs := multierr.New()
	errs.CollectIfNotNil(db.cache.Close())
	errs.CollectIfNotNil(db.pageManager.SyncFile())
	errs.CollectIfNotNil(db.file.Close())
	atomic.StoreUint32(&db.closed, 1)
	return errs.OrNil()
}

// Closed indicates, whether this file was closed.
func (db *DBFile) Closed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

// newDB creates a new DBFile from the given objects, and applies all options.
func newDB(file afero.File, mgr *PageManager, headerPage, tablesPage, configPage *page.Page, opts ...Option) (*DBFile, error) {
	db := &DBFile{
		log:       zerolog.Nop(),
		cacheSize: DefaultCacheSize,

		file:         file,
		pageManager:  mgr,
		headerPageID: headerPage.ID(),
		tablesPageID: tablesPage.ID(),
		configPageID: configPage.ID(),
	}
	for _, opt := range opts {
		opt(db)
	}

	db.cache = cache.NewLRUCache(db.cacheSize, mgr)

	return db, nil
}

func pointerCellValue(p *page.Page, cellKey string) (page.ID, error) {
	cell, ok := p.Cell([]byte(cellKey))
	if !ok {
		return 0, ErrNoSuchCell(cellKey)
	}
	if cell.Type() != page.CellTypePointer {
		return 0, fmt.Errorf("cell '%v' is %v, which is not a pointer cell", cellKey, cell.Type())
	}
	return cell.(page.PointerCell).Pointer, nil
}
