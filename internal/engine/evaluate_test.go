package engine

import (
	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func (suite *EngineSuite) TestFullTableScan() {
	_, err := suite.engine.evaluateCreateTable(suite.ctx, command.CreateTable{
		Name: "myTable",
		ColumnDefs: []command.ColumnDef{
			{
				Name: "myCol",
				Type: types.String,
			},
		},
	})
	suite.NoError(err)

	_, err = suite.engine.evaluateInsert(suite.ctx, command.Insert{
		Table: command.SimpleTable{
			Table: "myTable",
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.ConstantLiteral{Value: "myContent"}},
				{command.ConstantLiteral{Value: "mySecondContent"}},
				{command.ConstantLiteral{Value: "myThirdContent"}},
			},
		},
	})
	suite.NoError(err)

	tbl, err := suite.engine.Evaluate(command.Scan{
		Table: command.SimpleTable{
			Table: "myTable",
		},
	})
	suite.NoError(err)
	suite.EqualTables(table.NewInMemory(
		[]table.Col{
			{
				QualifiedName: "myCol",
				Type:          types.String,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("myContent")}},
			{Values: []types.Value{types.NewString("mySecondContent")}},
			{Values: []types.Value{types.NewString("myThirdContent")}},
		},
	), tbl)
}
