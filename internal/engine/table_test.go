package engine

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func TestTableSuite(t *testing.T) {
	suite.Run(t, new(TableSuite))
}

type TableSuite struct {
	EngineSuite
}

func (suite *TableSuite) TestCreateTable() {
	const tblName = "myTbl"

	suite.False(suite.ctx.tx.HasTable(tblName))

	result, err := suite.engine.evaluateCreateTable(suite.ctx, command.CreateTable{
		Name: tblName,
		ColumnDefs: []command.ColumnDef{
			{
				Name: "col1",
				Type: types.Integer,
			},
			{
				Name: "col2",
				Type: types.String,
			},
		},
	})
	suite.NoError(err)
	suite.EqualTables(table.Empty, result)
	suite.True(suite.ctx.tx.HasTable(tblName))

	tbl, err := suite.engine.LoadTable(suite.ctx.tx, tblName)
	suite.NoError(err)
	suite.EqualTables(table.NewInMemory(
		[]table.Col{
			{QualifiedName: "col1", Type: types.Integer},
			{QualifiedName: "col2", Type: types.String},
		},
		[]table.Row{},
	), tbl)
}

func (suite *TableSuite) TestEngine_LoadTable() {
	const (
		tableName = "myTable"
	)

	tbl, err := suite.engine.LoadTable(suite.ctx.tx, tableName)
	suite.EqualError(err, "table '"+tableName+"' does not exist")
	suite.Zero(tbl)

	_, err = suite.engine.evaluateCreateTable(suite.ctx, command.CreateTable{
		Name: tableName,
		ColumnDefs: []command.ColumnDef{
			{Name: "col1", Type: types.Integer},
			{Name: "col2", Type: types.String},
		},
	})
	suite.NoError(err)

	tbl, err = suite.engine.LoadTable(suite.ctx.tx, tableName)
	suite.NoError(err)
	suite.Equal(tableName, tbl.(Namer).Name())

}
