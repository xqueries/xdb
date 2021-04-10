package engine

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"

	"github.com/rs/zerolog"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/table"
)

var (
	byteOrder = binary.BigEndian
)

type timeProvider func() time.Time
type randomProvider func() int64

// Engine is the component that is used to evaluate commands.
type Engine struct {
	log      zerolog.Logger
	dbfs     *dbfs.DBFS
	profiler *profile.Profiler
	txmgr    TransactionManager

	timeProvider   timeProvider
	randomProvider randomProvider
}

// New creates a new engine object and applies the given options to it.
func New(dbfs *dbfs.DBFS, opts ...Option) (Engine, error) {
	e := Engine{
		log:   zerolog.Nop(),
		dbfs:  dbfs,
		txmgr: &brokenTransactionManager{},

		timeProvider: time.Now,
		randomProvider: func() int64 {
			buf := make([]byte, unsafe.Sizeof(int64(0))) // #nosec
			_, _ = rand.Read(buf)
			return int64(byteOrder.Uint64(buf))
		},
	}
	for _, opt := range opts {
		opt(&e)
	}

	if e.dbfs == nil {
		return Engine{}, fmt.Errorf("dbfs cannot be nil")
	}
	if e.txmgr == nil {
		return Engine{}, fmt.Errorf("txmgr cannot be nil")
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

	tx, err := e.txmgr.Start()
	if err != nil {
		return nil, fmt.Errorf("start new transaction: %w", err)
	}
	e.log.Trace().
		Stringer("tx", tx.ID()).
		Msg("start new transaction")

	resultTbl, err := e.EvaluateInTransaction(cmd, tx)

	if err != nil {
		return nil, fmt.Errorf("evaluate in transaction: %w", err)
	}

	if err := e.txmgr.Submit(tx); err != nil {
		return nil, fmt.Errorf("submit transaction: %w", err)
	}

	return resultTbl, nil
}

func (e Engine) EvaluateInTransaction(cmd command.Command, tx *Transaction) (table.Table, error) {
	ctx := newEmptyExecutionContext(tx)

	e.log.Debug().
		Str("ctx", ctx.String()).
		Stringer("tx", tx.ID()).
		Str("command", cmd.String()).
		Msg("evaluate")

	result, err := e.evaluate(ctx, cmd)
	if err != nil {
		return nil, fmt.Errorf("evaluate: %w", err)
	}

	return result, nil
}

// HasTable determines whether the engine has a table with the given name
// in the currently loaded database.
func (e Engine) HasTable(name string) bool {
	ok, err := e.dbfs.HasTable(name)
	return ok && err == nil
}

// Close closes the underlying database file.
func (e Engine) Close() error {
	defer e.profiler.Enter("close").Exit()

	var errs []error
	do := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	do(e.txmgr.Close())
	do(e.dbfs.Close())

	if len(errs) == 1 {
		return errs[0]
	} else if len(errs) != 0 {
		return fmt.Errorf("multiple errors: %v", errs)
	}
	return nil
}
