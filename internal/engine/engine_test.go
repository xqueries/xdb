package engine

import (
	"testing"

	"github.com/spf13/afero"
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

	fs := afero.NewMemMapFs()
	dbfs, err := dbfs.CreateNew(fs)
	suite.NoError(err)

	e, err := New(dbfs)
	suite.NoError(err)

	suite.engine = e
	suite.dbfs = dbfs

	tx, err := suite.engine.txmgr.Start()
	suite.Require().NoError(err)

	suite.ctx = newEmptyExecutionContext(tx)
}

func (suite *EngineSuite) EqualTables(expected, got table.Table) {
	suite.NotNil(got)
	expectedCols, err := expected.Cols()
	suite.NoError(err)
	gotCols, err := got.Cols()
	suite.NoError(err)
	suite.Equal(expectedCols, gotCols)
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
