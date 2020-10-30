package engine

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/types"
)

type evaluateExpressionTest struct {
	name    string
	e       Engine
	ctx     ExecutionContext
	expr    command.Expr
	want    types.Value
	wantErr string
}

func TestEngine_evaluateExpression(t *testing.T) {
	fixedTimestamp, err := time.Parse("2006-01-02T15:04:05", "2020-06-01T14:05:12")
	assert.NoError(t, err)
	fixedTimeProvider := func() time.Time { return fixedTimestamp }

	testEvaluateExpressionTest(t, []evaluateExpressionTest{
		{
			"nil",
			builder().build(),
			newEmptyExecutionContext(),
			nil,
			nil,
			"cannot evaluate expression of type <nil>",
		},
		{
			"true",
			builder().build(),
			newEmptyExecutionContext(),
			command.ConstantBooleanExpr{Value: true},
			types.NewBool(true),
			"",
		},
		{
			"false",
			builder().build(),
			newEmptyExecutionContext(),
			command.ConstantBooleanExpr{Value: false},
			types.NewBool(false),
			"",
		},
	})
	t.Run("functions", func(t *testing.T) {
		testEvaluateExpressionTest(t, []evaluateExpressionTest{
			{
				"function NOW",
				builder().
					timeProvider(fixedTimeProvider).
					build(),
				newEmptyExecutionContext(),
				command.FunctionExpr{
					Name: "NOW",
				},
				types.NewDate(fixedTimestamp),
				"",
			},
			{
				"unknown function",
				builder().build(),
				newEmptyExecutionContext(),
				command.FunctionExpr{
					Name: "NOTEXIST",
				},
				nil,
				"no function for name NOTEXIST(...)",
			}})
	})
	t.Run("arithmetic", func(t *testing.T) {
		t.Run("op=add", func(t *testing.T) {
			testEvaluateExpressionTest(t, []evaluateExpressionTest{
				{
					"simple integral addition",
					builder().build(),
					newEmptyExecutionContext(),
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
					newEmptyExecutionContext(),
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
					newEmptyExecutionContext(),
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
		t.Run("op=sub", func(t *testing.T) {
			testEvaluateExpressionTest(t, []evaluateExpressionTest{
				{
					"simple integral subtraction",
					builder().build(),
					newEmptyExecutionContext(),
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
					newEmptyExecutionContext(),
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
		t.Run("op=mul", func(t *testing.T) {
			testEvaluateExpressionTest(t, []evaluateExpressionTest{
				{
					"simple integral multiplication",
					builder().build(),
					newEmptyExecutionContext(),
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
					newEmptyExecutionContext(),
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
		t.Run("op=div", func(t *testing.T) {
			testEvaluateExpressionTest(t, []evaluateExpressionTest{
				{
					"simple integral division",
					builder().build(),
					newEmptyExecutionContext(),
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
					newEmptyExecutionContext(),
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
		t.Run("op=mod", func(t *testing.T) {
			testEvaluateExpressionTest(t, []evaluateExpressionTest{
				{
					"simple integral modulo",
					builder().build(),
					newEmptyExecutionContext(),
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
					newEmptyExecutionContext(),
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
		t.Run("op=pow", func(t *testing.T) {
			testEvaluateExpressionTest(t, []evaluateExpressionTest{
				{
					"simple integral exponentiation",
					builder().build(),
					newEmptyExecutionContext(),
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
					newEmptyExecutionContext(),
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

func testEvaluateExpressionTest(t *testing.T, tests []evaluateExpressionTest) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			got, err := tt.e.evaluateExpression(tt.ctx, tt.expr)
			assert.Equal(tt.want, got)
			if tt.wantErr != "" {
				assert.EqualError(err, tt.wantErr)
			} else {
				assert.NoError(err)
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
