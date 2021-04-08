package engine

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/profile"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func TestInsertSuite(t *testing.T) {
	suite.Run(t, new(InsertSuite))
}

type InsertSuite struct {
	EngineSuite
}

func (suite *InsertSuite) TestSimpleInsert() {
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

	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.ConstantBooleanExpr{Value: true},
		Input: command.Scan{
			Table: command.SimpleTable{
				Table: "myTable",
			},
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

func (suite *InsertSuite) TestManyInserts() {
	suite.T().Skip()

	p := profile.NewProfiler()
	suite.engine.profiler = p

	_, err := suite.engine.evaluateCreateTable(suite.ctx, command.CreateTable{
		Name: "myTable",
		ColumnDefs: []command.ColumnDef{
			{
				Name: "col1",
				Type: types.String,
			},
			{
				Name: "col2",
				Type: types.Integer,
			},
			{
				Name: "col3",
				Type: types.Integer,
			},
			{
				Name: "col4",
				Type: types.Bool,
			},
		},
	})
	suite.NoError(err)

	for i := 0; i < 100000; i++ {
		_, err = suite.engine.evaluateInsert(suite.ctx, command.Insert{
			Table: command.SimpleTable{
				Table: "myTable",
			},
			Input: command.Values{
				Values: [][]command.Expr{
					{
						command.ConstantLiteral{Value: "myContent" + strconv.Itoa(i)},
						command.ConstantLiteral{Numeric: true, Value: strconv.Itoa(i)},
						command.ConstantLiteral{Numeric: true, Value: strconv.Itoa(i * 19)},
						command.ConstantBooleanExpr{Value: i%17 == 0},
					},
				},
			},
		})
		suite.NoError(err)
	}

	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.ConstantBooleanExpr{Value: true},
		Input: command.Scan{
			Table: command.SimpleTable{
				Table: "myTable",
			},
		},
	})
	suite.NoError(err)

	suite.T().Log(p.Profile().String())

	toStringStart := time.Now()
	_, err = table.ToString(tbl)
	suite.NoError(err)
	suite.T().Logf("table.ToString(*) took %v", time.Since(toStringStart))
}
