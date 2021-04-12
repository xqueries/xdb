package engine

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/xqueries/xdb/internal/compiler"
	"github.com/xqueries/xdb/internal/engine/dbfs"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/parser"
)

func TestEngineSuite(t *testing.T) {
	suite.Run(t, new(EngineSuite))
}

type EngineSuite struct {
	suite.Suite

	ctx    ExecutionContext
	engine Engine
	dbfs   *dbfs.DBFS
}

func (suite *EngineSuite) SetupTest() {
	suite.ctx = newEmptyExecutionContext()
	suite.engine = createEngineOnEmptyDatabase(suite.T())
	suite.dbfs = suite.engine.dbfs
}

// createEngineOnEmptyDatabase creates a new, clean, ready to use in-memory database file
// together with a new engine that uses the fresh database. The result is a ready-to-use
// engine on a completely empty database file that is not on the OS's file system.
func createEngineOnEmptyDatabase(t assert.TestingT) Engine {
	assert := assert.New(t)

	// fs := afero.NewBasePathFs(afero.NewOsFs(), "testdata")
	fs := afero.NewMemMapFs()
	dbFile, err := dbfs.CreateNew(fs)
	assert.NoError(err)

	e, err := New(dbFile)
	assert.NoError(err)
	return e
}

func (suite *EngineSuite) EqualTables(expected, got table.Table) {
	suite.NotNil(got)
	suite.Equal(expected.Cols(), got.Cols())
	expectedIt, err := expected.Rows()
	suite.NoError(err)
	gotIt, err := got.Rows()
	suite.NoError(err)

	var expectedRows []table.Row
	var expectedErr error
	var gotRows []table.Row
	var gotErr error

	for {
		expectedNext, err := expectedIt.Next()
		expectedRows = append(expectedRows, expectedNext)
		expectedErr = err
		if err != nil {
			break
		}
	}
	for {
		gotNext, err := gotIt.Next()
		gotRows = append(gotRows, gotNext)
		gotErr = err
		if err != nil {
			break
		}
	}
	suite.EqualValues(expectedErr, gotErr)
	suite.Len(gotRows, len(expectedRows))

	for _, row := range gotRows {
		suite.Contains(expectedRows, row)
	}
}

// RunScript will run the given SQL script, which is useful for setting up a minimalistic
// database for a test.
// The test will fail if the script is incorrect or results in errors.
func (suite *EngineSuite) RunScript(sqlScript string) {
	p, err := parser.New(sqlScript)
	suite.NoError(err)

	c := compiler.New()

	for {
		next, errs, ok := p.Next()
		if !ok {
			break
		}
		suite.Len(errs, 0)

		cmd, err := c.Compile(next)
		suite.NoError(err)

		_, err = suite.engine.Evaluate(cmd)
		suite.NoError(err)
	}
}
