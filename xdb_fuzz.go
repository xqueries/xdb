// +build gofuzz

package xdb

import (
	"github.com/spf13/afero"

	"github.com/xqueries/xdb/internal/compiler"
	"github.com/xqueries/xdb/internal/engine"
	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/parser"
)

const (
	// DataNotInteresting indicates, that the input was not interesting, meaning
	// that the input was not valid and the parser handled detected and returned
	// an error. The input will still be added to the corpus.
	DataNotInteresting int = 0
	// DataInteresting indicates a valid parser input. The fuzzer should keep it
	// and modify it further.
	DataInteresting int = 1
	// Skip indicates, that the data must not be added to the corpus. You
	// probably shouldn't use it.
	Skip int = -1
)

func Fuzz(data []byte) int {
	statement := string(data)

	// try to parse the input
	p, err := parser.New(statement)
	if err != nil {
		return DataNotInteresting
	}
	stmt, errs, ok := p.Next()
	if !ok {
		return DataNotInteresting
	}
	if len(errs) != 0 {
		return DataNotInteresting
	}

	// compile the statement
	c := compiler.New()
	cmd, err := c.Compile(stmt)
	if err != nil {
		return DataNotInteresting
	}

	// create a new im-memory db file if none is set
	fs := afero.NewMemMapFs()

	dbFile, err := dbfs.CreateNew(fs)
	if err != nil {
		panic(err)
	}
	defer func() { _ = dbFile.Close() }()

	// fire up the engine
	e, err := engine.New(dbFile)
	if err != nil {
		panic(err)
	}

	result, err := e.Evaluate(cmd)
	if err != nil {
		return DataNotInteresting
	}

	_ = result
	return DataInteresting
}
