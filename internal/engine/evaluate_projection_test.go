package engine

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func TestProjectionSuite(t *testing.T) {
	suite.Run(t, new(ProjectionSuite))
}

type ProjectionSuite struct {
	suite.Suite

	ctx    ExecutionContext
	engine Engine
}

func (suite *ProjectionSuite) SetupTest() {
	suite.ctx = newEmptyExecutionContext()
	suite.engine = createEngineOnEmptyDatabase(suite.T())
}

func (suite *ProjectionSuite) TestEmptyProjection() {
	tbl, err := suite.engine.evaluateProjection(suite.ctx, command.Project{
		Cols: []command.Column{},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "world"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "bar"}, command.ConstantBooleanExpr{Value: false}},
			},
		},
	})
	suite.NoError(err)
	suite.Equal(table.Table{
		Cols: []table.Col{},
		Rows: []table.Row{},
	}, tbl)
}

func (suite *ProjectionSuite) TestSimpleProjection() {
	tbl, err := suite.engine.evaluateProjection(suite.ctx, command.Project{
		Cols: []command.Column{
			{Name: command.LiteralExpr{Value: "column2"}},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "world"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "bar"}, command.ConstantBooleanExpr{Value: false}},
			},
		},
	})
	suite.NoError(err)
	suite.Equal(table.Table{
		Cols: []table.Col{
			{
				QualifiedName: "column2",
				Type:          types.String,
			},
		},
		Rows: []table.Row{
			{Values: []types.Value{types.NewString("world")}},
			{Values: []types.Value{types.NewString("bar")}},
		},
	}, tbl)
}

func (suite *ProjectionSuite) TestSimpleProjectionWithAlias() {
	tbl, err := suite.engine.evaluateProjection(suite.ctx, command.Project{
		Cols: []command.Column{
			{
				Name:  command.LiteralExpr{Value: "column2"},
				Alias: "foo",
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "world"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "bar"}, command.ConstantBooleanExpr{Value: false}},
			},
		},
	})
	suite.NoError(err)
	suite.Equal(table.Table{
		Cols: []table.Col{
			{
				QualifiedName: "column2",
				Alias:         "foo",
				Type:          types.String,
			},
		},
		Rows: []table.Row{
			{Values: []types.Value{types.NewString("world")}},
			{Values: []types.Value{types.NewString("bar")}},
		},
	}, tbl)
}

func (suite *ProjectionSuite) TestSimpleProjectionWithMissingColumn() {
	tbl, err := suite.engine.evaluateProjection(suite.ctx, command.Project{
		Cols: []command.Column{
			{
				Name: command.LiteralExpr{Value: "foo"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "world"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "bar"}, command.ConstantBooleanExpr{Value: false}},
			},
		},
	})
	suite.EqualError(err, "no column with name or alias 'foo'")
	suite.Equal(table.Table{}, tbl)
}
