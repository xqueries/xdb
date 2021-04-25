package dbfs

import (
	"path/filepath"
	"strconv"
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

	dbfs, err := CreateNew(fs)
	suite.NoError(err)

	suite.NoError(Validate(fs))

	tblCount, err := dbfs.TableCount()
	suite.NoError(err)
	suite.Equal(0, tblCount)
}

func (suite *DBFSSuite) TestCreateTable() {
	fs := afero.NewMemMapFs()

	dbfs, err := CreateNew(fs)
	suite.NoError(err)

	infos, err := dbfs.loadTablesInfo()
	suite.NoError(err)
	suite.Equal(0, infos.Count)
	suite.Len(infos.Tables, 0)

	tbl, err := dbfs.CreateTable("myTable")
	suite.NoError(err)

	suite.NoError(Validate(fs))

	infos, err = dbfs.loadTablesInfo()
	suite.NoError(err)
	suite.EqualValues(TablesInfo{
		Tables: map[string]string{
			"myTable": tbl.id.String(),
		},
		Count: 1,
	}, infos)

	suite.NoError(Validate(fs))
}

func (suite *DBFSSuite) TestManyTables() {
	fs := afero.NewMemMapFs()

	dbfs, err := CreateNew(fs)
	suite.NoError(err)

	for i := 0; i < 100; i++ {
		infos, err := dbfs.loadTablesInfo()
		suite.NoError(err)
		suite.Equal(i, infos.Count)
		suite.Len(infos.Tables, i)

		tbl, err := dbfs.CreateTable("myTable" + strconv.Itoa(i))
		suite.NoError(err)

		infos, err = dbfs.loadTablesInfo()
		suite.NoError(err)
		suite.Equal(i+1, infos.Count)
		suite.Len(infos.Tables, i+1)

		tableDir := filepath.Join(TablesDirectory, tbl.id.String())
		suite.DirExists(fs, tableDir)
		suite.FileEmpty(fs, filepath.Join(tableDir, TableDataFile))
		suite.FileEmpty(fs, filepath.Join(tableDir, TableSchemaFile))
	}
}
