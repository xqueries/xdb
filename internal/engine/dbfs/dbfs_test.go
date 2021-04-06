package dbfs

import (
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"
)

func TestDBFSSuite(t *testing.T) {
	suite.Run(t, new(DBFSSuite))
}

type DBFSSuite struct {
	suite.Suite
}

func (suite *DBFSSuite) DirExists(fs afero.Fs, path string) {
	exists, err := afero.DirExists(fs, path)
	suite.NoError(err)
	suite.True(exists)
}

func (suite *DBFSSuite) FileExists(fs afero.Fs, path string) {
	exists, err := afero.Exists(fs, path)
	suite.NoError(err)
	isDir, err := afero.IsDir(fs, path)
	suite.NoError(err)
	suite.True(exists && !isDir)
}

func (suite *DBFSSuite) FileEmpty(fs afero.Fs, path string) {
	suite.FileExists(fs, path)
	empty, err := afero.IsEmpty(fs, path)
	suite.NoError(err)
	suite.True(empty)
}

func (suite *DBFSSuite) DirEmpty(fs afero.Fs, path string) {
	suite.DirExists(fs, path)
	empty, err := afero.IsEmpty(fs, path)
	suite.NoError(err)
	suite.True(empty)
}

func (suite *DBFSSuite) TestCreateNew() {
	fs := afero.NewMemMapFs()

	_, err := CreateNew(fs)
	suite.NoError(err)

	suite.DirExists(fs, TablesDirectory)
	suite.FileExists(fs, filepath.Join(TablesDirectory, TablesInfoFile))
	suite.FileEmpty(fs, filepath.Join(TablesDirectory, TablesInfoFile))
	files, err := afero.ReadDir(fs, TablesDirectory)
	suite.NoError(err)
	suite.Len(files, 1)
}

func (suite *DBFSSuite) TestCreateTable() {
	fs := afero.NewMemMapFs()

	dbfs, err := CreateNew(fs)
	suite.NoError(err)

	tbl, err := dbfs.CreateTable("myTable")
	suite.NoError(err)

	tableDir := filepath.Join(TablesDirectory, tbl.id.String())
	suite.DirExists(fs, tableDir)
	suite.FileEmpty(fs, filepath.Join(tableDir, TableDataFile))
	suite.FileEmpty(fs, filepath.Join(tableDir, TableSchemaFile))
}
