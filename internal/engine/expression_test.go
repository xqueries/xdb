package engine

import (
	"time"

	"github.com/rs/zerolog"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/types"
)

type evaluateExpressionTest struct {
	name    string
	e       Engine
	expr    command.Expr
	want    types.Value
	wantErr string
}

func (suite *EngineSuite) TestEvaluateExpression() {
	fixedTimestamp, err := time.Parse("2006-01-02T15:04:05", "2020-06-01T14:05:12")
	fixedTimeProvider := func() time.Time { return fixedTimestamp }
	suite.NoError(err)

	suite.testEvaluateExpressionTest([]evaluateExpressionTest{
		{
			"nil",
			builder().build(),
			nil,
			nil,
			"cannot evaluate expression of type <nil>",
		},
		{
			"true",
			builder().build(),
			command.ConstantBooleanExpr{Value: true},
			types.NewBool(true),
			"",
		},
		{
			"false",
			builder().build(),
			command.ConstantBooleanExpr{Value: false},
			types.NewBool(false),
			"",
		},
	})
	suite.Run("functions", func() {
		suite.testEvaluateExpressionTest([]evaluateExpressionTest{
			{
				"function NOW",
				builder().
					timeProvider(fixedTimeProvider).
					build(),
				command.FunctionExpr{
					Name: "NOW",
				},
				types.NewDate(fixedTimestamp),
				"",
			},
			{
				"unknown function",
				builder().build(),
				command.FunctionExpr{
					Name: "NOTEXIST",
				},
				nil,
				"no function for name NOTEXIST(...)",
			}})
	})
	suite.Run("arithmetic", func() {
		suite.Run("op=add", func() {
			suite.testEvaluateExpressionTest([]evaluateExpressionTest{
				{
					"simple integral addition",
					builder().build(),
					command.AddExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "5", Numeric: true},
							Right: command.ConstantLiteral{Value: "6", Numeric: true},
						},
					},
					types.NewInteger(11),
					"",
				},
				{
					"simple real addition",
					builder().build(),
					command.AddExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "5.5", Numeric: true},
							Right: command.ConstantLiteral{Value: "6.7", Numeric: true},
						},
					},
					types.NewReal(12.2),
					"",
				},
				{
					"simple string addition/concatenation",
					builder().build(),
					command.AddExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "abc"},
							Right: command.ConstantLiteral{Value: "def"},
						},
					},
					types.NewString("abcdef"),
					"",
				},
			})
		})
		suite.Run("op=sub", func() {
			suite.testEvaluateExpressionTest([]evaluateExpressionTest{
				{
					"simple integral subtraction",
					builder().build(),
					command.SubExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "6", Numeric: true},
							Right: command.ConstantLiteral{Value: "5", Numeric: true},
						},
					},
					types.NewInteger(1),
					"",
				},
				{
					"simple real subtraction",
					builder().build(),
					command.SubExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "12.2", Numeric: true},
							Right: command.ConstantLiteral{Value: "7.6", Numeric: true},
						},
					},
					types.NewReal(4.6),
					"",
				},
			})
		})
		suite.Run("op=mul", func() {
			suite.testEvaluateExpressionTest([]evaluateExpressionTest{
				{
					"simple integral multiplication",
					builder().build(),
					command.MulExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "6", Numeric: true},
							Right: command.ConstantLiteral{Value: "5", Numeric: true},
						},
					},
					types.NewInteger(30),
					"",
				},
				{
					"simple real multiplication",
					builder().build(),
					command.MulExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "6.2", Numeric: true},
							Right: command.ConstantLiteral{Value: "5.7", Numeric: true},
						},
					},
					types.NewReal(35.34),
					"",
				},
			})
		})
		suite.Run("op=div", func() {
			suite.testEvaluateExpressionTest([]evaluateExpressionTest{
				{
					"simple integral division",
					builder().build(),
					command.DivExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "15", Numeric: true},
							Right: command.ConstantLiteral{Value: "5", Numeric: true},
						},
					},
					types.NewReal(3),
					"",
				},
				{
					"simple real division",
					builder().build(),
					command.DivExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "35.34", Numeric: true},
							Right: command.ConstantLiteral{Value: "5.7", Numeric: true},
						},
					},
					types.NewReal(6.2),
					"",
				},
			})
		})
		suite.Run("op=mod", func() {
			suite.testEvaluateExpressionTest([]evaluateExpressionTest{
				{
					"simple integral modulo",
					builder().build(),
					command.ModExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "7", Numeric: true},
							Right: command.ConstantLiteral{Value: "5", Numeric: true},
						},
					},
					types.NewInteger(2),
					"",
				},
				{
					"real modulo",
					builder().build(),
					command.ModExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "7.2", Numeric: true},
							Right: command.ConstantLiteral{Value: "5.2", Numeric: true},
						},
					},
					nil,
					"Real does not support modulo",
				},
			})
		})
		suite.Run("op=pow", func() {
			suite.testEvaluateExpressionTest([]evaluateExpressionTest{
				{
					"simple integral exponentiation",
					builder().build(),
					command.PowExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "2", Numeric: true},
							Right: command.ConstantLiteral{Value: "4", Numeric: true},
						},
					},
					types.NewInteger(16),
					"",
				},
				{
					"simple real exponentiation",
					builder().build(),
					command.PowExpression{
						BinaryBase: command.BinaryBase{
							Left:  command.ConstantLiteral{Value: "2.2", Numeric: true},
							Right: command.ConstantLiteral{Value: "1.5", Numeric: true},
						},
					},
					types.NewReal(3.2631273343220926),
					"",
				},
			})
		})
	})
}

func (suite *EngineSuite) testEvaluateExpressionTest(tests []evaluateExpressionTest) {
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			got, err := tt.e.evaluateExpression(suite.ctx, tt.expr)
			suite.Equal(tt.want, got)
			if tt.wantErr != "" {
				suite.EqualError(err, tt.wantErr)
			} else {
				suite.NoError(err)
			}
		})
	}
}

type engineBuilder struct {
	e Engine
}

func builder() engineBuilder {
	return engineBuilder{
		Engine{
			log: zerolog.Nop(),
		},
	}
}

func (b engineBuilder) timeProvider(tp timeProvider) engineBuilder {
	b.e.timeProvider = tp
	return b
}

func (b engineBuilder) build() Engine {
	return b.e
}
