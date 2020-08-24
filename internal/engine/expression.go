package engine

import (
	"fmt"
	"strconv"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/types"
)

// evaluateExpression evaluates the given expression to a runtime-constant
// value, meaning that it can only be evaluated to a constant value with a given
// execution context.
func (e Engine) evaluateExpression(ctx ExecutionContext, expr command.Expr) (types.Value, error) {
	if expr == nil {
		return nil, fmt.Errorf("cannot evaluate expression of type %T", expr)
	}

	switch ex := expr.(type) {
	case command.BinaryExpression:
		return e.evaluateBinaryExpr(ctx, ex)
	case command.ConstantBooleanExpr:
		return types.NewBool(ex.Value), nil
	case command.LiteralExpr:
		return e.evaluateLiteralExpr(ctx, ex)
	case command.FunctionExpr:
		return e.evaluateFunctionExpr(ctx, ex)
	}
	return nil, ErrUnimplemented(fmt.Sprintf("evaluate %T", expr))
}

func (e Engine) evaluateMultipleExpressions(ctx ExecutionContext, exprs []command.Expr) ([]types.Value, error) {
	var vals []types.Value
	for _, expr := range exprs {
		evaluated, err := e.evaluateExpression(ctx, expr)
		if err != nil {
			return nil, err
		}
		vals = append(vals, evaluated)
	}
	return vals, nil
}

// evaluateLiteralExpr evaluates the given literal expression based on the
// current execution context. The returned value will either be a numeric value
// (integer or real) or a string value.
func (e Engine) evaluateLiteralExpr(ctx ExecutionContext, expr command.LiteralExpr) (types.Value, error) {
	// Check whether the expression value is a numeric literal. In the future,
	// this evaluation might depend on the execution context.
	if numVal, ok := ToNumericValue(expr.Value); ok {
		return numVal, nil
	}
	value := expr.Value
	// if not a numeric literal, remove quotes and resolve escapes
	resolved, err := strconv.Unquote(expr.Value)
	if err == nil {
		value = resolved
	}
	if val, ok := ctx.intermediateRow.ValueForColName(value); ok {
		return val, nil
	}
	return types.NewString(value), nil
}

func (e Engine) evaluateFunctionExpr(ctx ExecutionContext, expr command.FunctionExpr) (types.Value, error) {
	exprs, err := e.evaluateMultipleExpressions(ctx, expr.Args)
	if err != nil {
		return nil, fmt.Errorf("arguments: %w", err)
	}

	function := types.NewFunction(expr.Name, exprs...)
	return e.evaluateFunction(ctx, function)
}

func (e Engine) evaluateBinaryExpr(ctx ExecutionContext, expr command.BinaryExpression) (types.Value, error) {
	left, err := e.evaluateExpression(ctx, expr.LeftExpr())
	if err != nil {
		return nil, fmt.Errorf("left: %w", err)
	}
	right, err := e.evaluateExpression(ctx, expr.RightExpr())
	if err != nil {
		return nil, fmt.Errorf("right: %w", err)
	}

	switch expr.(type) {
	case command.EqualityExpr:
		return types.NewBool(e.eq(left, right)), nil
	case command.LessThanExpr:
		return types.NewBool(e.lt(left, right)), nil
	case command.GreaterThanExpr:
		return types.NewBool(e.gt(left, right)), nil
	case command.LessThanOrEqualToExpr:
		return types.NewBool(e.lteq(left, right)), nil
	case command.GreaterThanOrEqualToExpr:
		return types.NewBool(e.gteq(left, right)), nil
	case command.AddExpression:
		return e.add(ctx, left, right)
	case command.SubExpression:
		return e.sub(ctx, left, right)
	case command.MulExpression:
		return e.mul(ctx, left, right)
	case command.DivExpression:
		return e.div(ctx, left, right)
	case command.ModExpression:
		return e.mod(ctx, left, right)
	case command.PowExpression:
		return e.pow(ctx, left, right)
	}
	return nil, ErrUnimplemented(fmt.Sprintf("%T", expr))
}
