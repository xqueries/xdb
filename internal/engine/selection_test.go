package engine

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func TestSelectionSuite(t *testing.T) {
	suite.Run(t, new(SelectionSuite))
}

type SelectionSuite struct {
	EngineSuite
}

func (suite *SelectionSuite) TestTrivialSelection() {
	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.ConstantBooleanExpr{Value: true},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "5"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "7"}, command.ConstantBooleanExpr{Value: false}},
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
				QualifiedName: "column2",
				Type:          types.Integer,
			},
			{
				QualifiedName: "column3",
				Type:          types.Bool,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("hello"), types.NewInteger(5), types.NewBool(true)}},
			{Values: []types.Value{types.NewString("foo"), types.NewInteger(7), types.NewBool(false)}},
		},
	), tbl)
}

func (suite *SelectionSuite) TestSimpleSelection() {
	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.EqualityExpr{
			BinaryBase: command.BinaryBase{
				Left:  command.LiteralExpr{Value: "column2"},
				Right: command.LiteralExpr{Value: "7"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "5"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "7"}, command.ConstantBooleanExpr{Value: false}},
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
				QualifiedName: "column2",
				Type:          types.Integer,
			},
			{
				QualifiedName: "column3",
				Type:          types.Bool,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("foo"), types.NewInteger(7), types.NewBool(false)}},
		},
	), tbl)
}

func (suite *SelectionSuite) TestComparingSelectionGreaterThan() {
	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.GreaterThanExpr{
			BinaryBase: command.BinaryBase{
				Left:  command.LiteralExpr{Value: "column2"},
				Right: command.LiteralExpr{Value: "5"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "5"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "7"}, command.ConstantBooleanExpr{Value: false}},
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
				QualifiedName: "column2",
				Type:          types.Integer,
			},
			{
				QualifiedName: "column3",
				Type:          types.Bool,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("foo"), types.NewInteger(7), types.NewBool(false)}},
		},
	), tbl)
}

func (suite *SelectionSuite) TestComparingSelectionLessThan() {
	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.LessThanExpr{
			BinaryBase: command.BinaryBase{
				Left:  command.LiteralExpr{Value: "column2"},
				Right: command.LiteralExpr{Value: "7"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "5"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "7"}, command.ConstantBooleanExpr{Value: false}},
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
				QualifiedName: "column2",
				Type:          types.Integer,
			},
			{
				QualifiedName: "column3",
				Type:          types.Bool,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("hello"), types.NewInteger(5), types.NewBool(true)}},
		},
	), tbl)
}

func (suite *SelectionSuite) TestComparingSelectionGreaterThanOrEqualTo() {
	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.GreaterThanOrEqualToExpr{
			BinaryBase: command.BinaryBase{
				Left:  command.LiteralExpr{Value: "column2"},
				Right: command.LiteralExpr{Value: "5"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "5"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "7"}, command.ConstantBooleanExpr{Value: false}},
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
				QualifiedName: "column2",
				Type:          types.Integer,
			},
			{
				QualifiedName: "column3",
				Type:          types.Bool,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("hello"), types.NewInteger(5), types.NewBool(true)}},
			{Values: []types.Value{types.NewString("foo"), types.NewInteger(7), types.NewBool(false)}},
		},
	), tbl)
}

func (suite *SelectionSuite) TestComparingSelectionLessThanOrEqualTo() {
	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.LessThanOrEqualToExpr{
			BinaryBase: command.BinaryBase{
				Left:  command.LiteralExpr{Value: "column2"},
				Right: command.LiteralExpr{Value: "7"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "5"}, command.ConstantBooleanExpr{Value: true}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "7"}, command.ConstantBooleanExpr{Value: false}},
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
				QualifiedName: "column2",
				Type:          types.Integer,
			},
			{
				QualifiedName: "column3",
				Type:          types.Bool,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("hello"), types.NewInteger(5), types.NewBool(true)}},
			{Values: []types.Value{types.NewString("foo"), types.NewInteger(7), types.NewBool(false)}},
		},
	), tbl)
}

func (suite *SelectionSuite) TestComparingColumns() {
	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.EqualityExpr{
			BinaryBase: command.BinaryBase{
				Left:  command.LiteralExpr{Value: "column1"},
				Right: command.LiteralExpr{Value: "column2"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "world"}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "foo"}},
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
				QualifiedName: "column2",
				Type:          types.String,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("foo"), types.NewString("foo")}},
		},
	), tbl)
}

func (suite *SelectionSuite) TestComparingColumnAgainstString() {
	tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
		Filter: command.EqualityExpr{
			BinaryBase: command.BinaryBase{
				Left:  command.LiteralExpr{Value: "column2"},
				Right: command.LiteralExpr{Value: "world"},
			},
		},
		Input: command.Values{
			Values: [][]command.Expr{
				{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "world"}},
				{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "foo"}},
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
				QualifiedName: "column2",
				Type:          types.String,
			},
		},
		[]table.Row{
			{Values: []types.Value{types.NewString("hello"), types.NewString("world")}},
		},
	), tbl)
}

func (suite *SelectionSuite) TestInvalidFilter() {
	suite.Run("invalid expression", func() {
		tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
			Filter: command.LiteralExpr{Value: "invalid"},
			Input: command.Values{
				Values: [][]command.Expr{
					{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "5"}, command.ConstantBooleanExpr{Value: true}},
					{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "7"}, command.ConstantBooleanExpr{Value: false}},
				},
			},
		})
		suite.EqualError(err, "cannot use command.LiteralExpr as filter")
		suite.Zero(tbl)
	})
	suite.Run("invalid binary expression", func() {
		tbl, err := suite.engine.evaluateSelection(suite.ctx, command.Select{
			Filter: command.AddExpression{
				BinaryBase: command.BinaryBase{
					Left:  command.LiteralExpr{Value: "5"},
					Right: command.LiteralExpr{Value: "12"},
				},
			},
			Input: command.Values{
				Values: [][]command.Expr{
					{command.LiteralExpr{Value: "hello"}, command.LiteralExpr{Value: "5"}, command.ConstantBooleanExpr{Value: true}},
					{command.LiteralExpr{Value: "foo"}, command.LiteralExpr{Value: "7"}, command.ConstantBooleanExpr{Value: false}},
				},
			},
		})
		suite.EqualError(err, "cannot use command.AddExpression as filter")
		suite.Nil(tbl)
	})
}
