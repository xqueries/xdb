package engine

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"github.com/rs/zerolog"
	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/storage"
	"github.com/xqueries/xdb/internal/engine/storage/cache"
	"github.com/xqueries/xdb/internal/engine/table"
)

var (
	byteOrder = binary.BigEndian
)

type timeProvider func() time.Time
type randomProvider func() int64

// Engine is the component that is used to evaluate commands.
type Engine struct {
	log       zerolog.Logger
	dbFile    *storage.DBFile
	pageCache cache.Cache
	profiler  *profile.Profiler

	timeProvider   timeProvider
	randomProvider randomProvider

	tablesPageContainer PageContainer
}

// New creates a new engine object and applies the given options to it.
func New(dbFile *storage.DBFile, opts ...Option) (Engine, error) {
	e := Engine{
		log:       zerolog.Nop(),
		dbFile:    dbFile,
		pageCache: dbFile.Cache(),

		timeProvider:   time.Now,
		randomProvider: func() int64 { return int64(rand.Uint64()) },
	}
	e.tablesPageContainer = e.NewPageContainer(e.dbFile.TablesPageID())
	for _, opt := range opts {
		opt(&e)
	}
	return e, nil
}

// Evaluate evaluates the given command. This may mutate the state of the
// database, and changes may occur to the database file.
func (e Engine) Evaluate(cmd command.Command) (table.Table, error) {
	_ = e.eq
	_ = e.lt
	_ = e.gt
	_ = e.lteq
	_ = e.gteq
	_ = e.builtinCount
	_ = e.builtinUCase
	_ = e.builtinLCase
	_ = e.builtinMin
	_ = e.builtinMax

	ctx := newEmptyExecutionContext()

	e.log.Debug().
		Str("ctx", ctx.String()).
		Str("command", cmd.String()).
		Msg("evaluate")

	result, err := e.evaluate(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("evaluate: %w", err)
	}
	return result, nil
}

// HasTable determines whether the engine has a table with the given name
// in the currently loaded database file.
func (e Engine) HasTable(name string) bool {
	tablesPageID := e.dbFile.TablesPageID()
	tablesPage, err := e.pageCache.FetchAndPin(tablesPageID)
	if err != nil {
		return false
	}
	defer e.pageCache.Unpin(tablesPageID)

	_, ok := tablesPage.Cell([]byte(name))
	return ok
}

// Closed determines whether the underlying database file was closed. If so,
// this engine is considered closed, as it can no longer operate on the
// underlying file.
func (e Engine) Closed() bool {
	return e.dbFile.Closed()
}

// Close closes the underlying database file.
func (e Engine) Close() error {
	defer e.profiler.Enter("close").Exit()

	return e.dbFile.Close()
}
