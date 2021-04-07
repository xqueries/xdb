package test

import (
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/xqueries/xdb/internal/compiler"
	"github.com/xqueries/xdb/internal/engine"
	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/parser"
)

var (
	overwriteExpected bool
)

type Test struct {
	Name string

	CompileOptions []compiler.Option
	EngineOptions  []engine.Option
	DBFileName     string

	Statement string
}

func TestMain(m *testing.M) {
	flag.BoolVar(&overwriteExpected, "update", false, "overwrite / update expected output files")
	flag.Parse()

	os.Exit(m.Run())
}

func RunAndCompare(t *testing.T, tt Test) {
	t.Helper()
	t.Run(tt.Name, func(t *testing.T) {
		t.Helper()
		runAndCompare(t, tt)
	})
}

func runAndCompare(t *testing.T, tt Test) {
	t.Helper()
	t.Logf("statement: %v", tt.Statement)

	assert := assert.New(t)

	if overwriteExpected {
		t.Log("OVERWRITING EXPECTED FILE")
		t.Fail()
	}

	// create a new im-memory db file if none is set
	fs := afero.NewMemMapFs()

	dbfs, err := dbfs.CreateNew(fs)
	assert.NoError(err)

	totalStart := time.Now()
	parseStart := time.Now()

	p, err := parser.New(tt.Statement)
	assert.NoError(err)

	stmt, errs, ok := p.Next()
	assert.True(ok)
	for _, err := range errs {
		assert.NoError(err, "parse")
	}

	t.Logf("parse: %v", time.Since(parseStart))

	compileStart := time.Now()

	c := compiler.New(tt.CompileOptions...)
	cmd, err := c.Compile(stmt)
	assert.NoError(err, "compile")

	t.Logf("compile: %v", time.Since(compileStart))

	engineStart := time.Now()

	e, err := engine.New(dbfs, tt.EngineOptions...)
	assert.NoError(err, "create engine")

	t.Logf("start engine: %v", time.Since(engineStart))

	evalStart := time.Now()

	result, err := e.Evaluate(cmd)
	assert.NoError(err, "evaluate")

	t.Logf("evaluate: %v", time.Since(evalStart))
	t.Logf("TOTAL: %v", time.Since(totalStart))

	tableString, err := table.ToString(result)
	assert.NoError(err)

	assert.NoError(e.Close())

	t.Logf("evaluation result:\n%v", tableString)

	if overwriteExpected {
		writeExpectedFile(t, tt.Name, tableString)
	} else {
		expectedData := loadExpectedFile(t, tt.Name)
		assert.Equal(string(expectedData), tableString)
	}
}

func writeExpectedFile(t *testing.T, testName string, output string) {
	assert := assert.New(t)

	fs := afero.NewBasePathFs(afero.NewOsFs(), "testdata")

	err := fs.MkdirAll(testName, 0700)
	assert.NoError(err)

	f, err := fs.Create(filepath.Join(testName, "output"))
	assert.NoError(err)
	defer func() { _ = f.Close() }()

	_, err = f.WriteString(output)
	assert.NoError(err)
}

func loadExpectedFile(t *testing.T, testName string) []byte {
	assert := assert.New(t)

	fs := afero.NewBasePathFs(afero.NewOsFs(), "testdata")
	f, err := fs.Open(filepath.Join(testName, "output"))
	assert.NoError(err)
	defer func() { _ = f.Close() }()

	data, err := ioutil.ReadAll(f)
	assert.NoError(err)

	return data
}
