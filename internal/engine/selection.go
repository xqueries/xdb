package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func (e Engine) evaluateSelection(ctx ExecutionContext, sel command.Select) (table.Table, error) {
	defer e.profiler.Enter("selection").Exit()

	origin, err := e.evaluateList(ctx, sel.Input)
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}

	// filter might have been optimized to constant expression
	if expr, ok := sel.Filter.(command.ConstantBooleanExpr); ok && expr.Value {
		return origin, nil
	}

	switch t := sel.Filter.(type) {
	default:
		return nil, fmt.Errorf("cannot use %T as filter", t)
	case command.EqualityExpr, command.GreaterThanExpr, command.GreaterThanOrEqualToExpr, command.LessThanExpr, command.LessThanOrEqualToExpr:
	}

	return table.NewFilteredRow(origin, func(r table.RowWithColInfo) (bool, error) {
		defer e.profiler.Enter("selection (lazy)").Exit()
		switch filter := sel.Filter.(type) {
		case command.BinaryExpression:
			val, err := e.evaluateBinaryExpr(ctx.IntermediateRow(r), filter)
			if err != nil {
				return false, err
			}
			if !val.Is(types.Bool) {
				return false, fmt.Errorf("expression does not evaluate to bool")
			}
			return val.(types.BoolValue).Value, nil
		}
		return false, nil
	}), nil
}
