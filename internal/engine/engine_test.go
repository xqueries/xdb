package engine

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/xqueries/xdb/internal/engine/storage"
	"github.com/xqueries/xdb/internal/engine/table"
)

func TestEngineSuite(t *testing.T) {
	suite.Run(t, new(EngineSuite))
}

type EngineSuite struct {
	suite.Suite

	ctx    ExecutionContext
	engine Engine
	dbFile *storage.DBFile
}

func (suite *EngineSuite) SetupTest() {
	suite.ctx = newEmptyExecutionContext()
	suite.engine = createEngineOnEmptyDatabase(suite.T())
	suite.dbFile = suite.engine.dbFile
}

// createEngineOnEmptyDatabase creates a new, clean, ready to use in-memory database file
// together with a new engine that uses the fresh database. The result is a ready-to-use
// engine on a completely empty database file that is not on the OS's file system.
func createEngineOnEmptyDatabase(t assert.TestingT) Engine {
	assert := assert.New(t)

	fs := afero.NewMemMapFs()
	f, err := fs.Create("mydbfile")
	assert.NoError(err)
	dbFile, err := storage.Create(f)
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
	for {
		expectedNext, expectedErr := expectedIt.Next()
		gotNext, gotErr := gotIt.Next()
		suite.EqualValues(expectedErr, gotErr)
		suite.Equal(expectedNext, gotNext)
		if !(expectedErr == nil && gotErr == nil) {
			break
		}
	}
}
