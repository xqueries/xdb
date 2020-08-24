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
		return table.Table{}, fmt.Errorf("list: %w", err)
	}

	// filter might have been optimized to constant expression
	if expr, ok := sel.Filter.(command.ConstantBooleanExpr); ok && expr.Value {
		return origin, nil
	}

	switch t := sel.Filter.(type) {
	default:
		return table.Table{}, fmt.Errorf("cannot use %T as filter", t)
	case command.BinaryExpression:
	}

	newTable, err := origin.FilterRows(func(r table.RowWithColInfo) (bool, error) {
		switch filter := sel.Filter.(type) {
		case command.BinaryExpression:
			val, err := e.evaluateBinaryExpr(ctx.IntermediateRow(r), filter)
			if err != nil {
				return false, fmt.Errorf("binary expression: %w", err)
			}
			if !val.Is(types.Bool) {
				return false, fmt.Errorf("only Bool expressions allowed as filter, got %v", val.Type().Name())
			}
			return val.(types.BoolValue).Value, nil
		}
		return false, nil
	})
	if err != nil {
		return table.Table{}, fmt.Errorf("filter: %w", err)
	}

	return newTable, nil
}
