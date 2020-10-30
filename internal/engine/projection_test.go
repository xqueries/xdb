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
	EngineSuite
}

func (suite *ProjectionSuite) TestEmptyProjection() {
	tbl, err := suite.engine.evaluateProjection(suite.ctx, command.Project{
		Cols: []command.Column{},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.ConstantLiteral{Value: "hello"}, command.ConstantLiteral{Value: "world"}, command.ConstantBooleanExpr{Value: true}},
				{command.ConstantLiteral{Value: "foo"}, command.ConstantLiteral{Value: "bar"}, command.ConstantBooleanExpr{Value: false}},
			},
		},
	})
	suite.NoError(err)
	suite.EqualTables(table.Empty, tbl)
}

func (suite *ProjectionSuite) TestSimpleProjection() {
	tbl, err := suite.engine.evaluateProjection(suite.ctx, command.Project{
		Cols: []command.Column{
			{Expr: command.ColumnReference{Name: "column2"}},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.ConstantLiteral{Value: "hello"}, command.ConstantLiteral{Value: "world"}, command.ConstantBooleanExpr{Value: true}},
				{command.ConstantLiteral{Value: "foo"}, command.ConstantLiteral{Value: "bar"}, command.ConstantBooleanExpr{Value: false}},
			},
		},
	})
	suite.NoError(err)
	suite.EqualTables(table.NewInMemory(
		[]table.Col{
			{
				QualifiedName: "column2",
				Type:          types.String,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("world")}},
			{Values: []types.Value{types.NewString("bar")}},
		},
	), tbl)
}

func (suite *ProjectionSuite) TestSimpleProjectionWithAlias() {
	tbl, err := suite.engine.evaluateProjection(suite.ctx, command.Project{
		Cols: []command.Column{
			{
				Expr:  command.ColumnReference{Name: "column2"},
				Alias: "foo",
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.ConstantLiteral{Value: "hello"}, command.ConstantLiteral{Value: "world"}, command.ConstantBooleanExpr{Value: true}},
				{command.ConstantLiteral{Value: "foo"}, command.ConstantLiteral{Value: "bar"}, command.ConstantBooleanExpr{Value: false}},
			},
		},
	})
	suite.NoError(err)
	suite.EqualTables(table.NewInMemory(
		[]table.Col{
			{
				QualifiedName: "column2",
				Alias:         "foo",
				Type:          types.String,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("world")}},
			{Values: []types.Value{types.NewString("bar")}},
		},
	), tbl)
}

func (suite *ProjectionSuite) TestSimpleProjectionWithMissingColumn() {
	tbl, err := suite.engine.evaluateProjection(suite.ctx, command.Project{
		Cols: []command.Column{
			{
				Expr: command.ColumnReference{Name: "foo"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.ConstantLiteral{Value: "hello"}, command.ConstantLiteral{Value: "world"}, command.ConstantBooleanExpr{Value: true}},
				{command.ConstantLiteral{Value: "foo"}, command.ConstantLiteral{Value: "bar"}, command.ConstantBooleanExpr{Value: false}},
			},
		},
	})
	suite.EqualError(err, "no column with name or alias 'foo'")
	suite.Nil(tbl)
}

func (suite *ProjectionSuite) TestSimpleProjectionWithColumnAndConstant() {
	tbl, err := suite.engine.evaluateProjection(suite.ctx, command.Project{
		Cols: []command.Column{
			{
				Expr: command.ColumnReference{Name: "column1"},
			},
			{
				Expr: command.ConstantLiteralOrColumnReference{ValueOrName: "column4"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.ConstantLiteral{Value: "hello"}, command.ConstantLiteral{Value: "world"}, command.ConstantBooleanExpr{Value: true}},
				{command.ConstantLiteral{Value: "foo"}, command.ConstantLiteral{Value: "bar"}, command.ConstantBooleanExpr{Value: false}},
			},
		},
	})
	suite.NoError(err)
	suite.EqualTables(table.NewInMemory(
		[]table.Col{
			{
				QualifiedName: "column1",
				Type:          types.String,
			},
			{
				QualifiedName: "column4",
				Type:          types.String,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("hello"), types.NewString("column4")}},
		},
	), tbl)
}
