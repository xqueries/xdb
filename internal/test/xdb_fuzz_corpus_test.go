package test

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/xqueries/xdb/internal/compiler"
	"github.com/xqueries/xdb/internal/engine"
	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/parser"
)

const (
	fuzzCorpusDir = "testdata/fuzz/corpus"
)

// TestFuzzCorpus runs the current fuzzing corpus.
func TestFuzzCorpus(t *testing.T) {
	assert := assert.New(t)

	corpusFiles, err := filepath.Glob(filepath.Join(fuzzCorpusDir, "*"))
	assert.NoError(err)

	for _, corpusFile := range corpusFiles {
		t.Run(filepath.Base(corpusFile), _TestCorpusFile(corpusFile))
	}
}

func _TestCorpusFile(file string) func(*testing.T) {
	return func(t *testing.T) {
		assert := assert.New(t)

		data, err := ioutil.ReadFile(file)
		assert.NoError(err)
		content := string(data)

		// try to parse the input
		p, err := parser.New(content)
		if err != nil {
			return
		}

		stmt, errs, ok := p.Next()
		if !ok || len(errs) != 0 {
			return
		}

		// compile the statement
		c := compiler.New()
		cmd, err := c.Compile(stmt)
		if err != nil {
			return
		}

		// create a new im-memory db
		fs := afero.NewMemMapFs()

		dbFile, err := dbfs.CreateNew(fs)
		assert.NoError(err)
		defer func() { _ = dbFile.Close() }()

		// fire up the engine
		e, err := engine.New(dbFile)
		assert.NoError(err)

		result, err := e.Evaluate(cmd)
		if err != nil {
			return
		}
		_ = result
	}
}
