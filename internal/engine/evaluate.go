package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
)

func (e Engine) evaluate(ctx ExecutionContext, c command.Command) (table.Table, error) {
	switch cmd := c.(type) {
	case command.List:
		return e.evaluateList(ctx, cmd)
	}
	return table.Table{}, ErrUnimplemented(c)
}

func (e Engine) evaluateList(ctx ExecutionContext, l command.List) (table.Table, error) {
	switch list := l.(type) {
	case command.Values:
		values, err := e.evaluateValues(ctx, list)
		if err != nil {
			return table.Table{}, fmt.Errorf("values: %w", err)
		}
		return values, nil
	case command.Scan:
		scanned, err := e.evaluateScan(ctx, list)
		if err != nil {
			return table.Table{}, fmt.Errorf("scan: %w", err)
		}
		return scanned, nil
	case command.Project:
		return e.evaluateProjection(ctx, list)
	case command.Select:
		return e.evaluateSelection(ctx, list)
	}
	return table.Table{}, ErrUnimplemented(l)
}

func (e Engine) evaluateScan(ctx ExecutionContext, s command.Scan) (table.Table, error) {
	defer e.profiler.Enter("scan").Exit()

	switch tbl := s.Table.(type) {
	case command.SimpleTable:
		return e.scanSimpleTable(ctx, tbl)
	default:
		return table.Table{}, ErrUnimplemented(fmt.Sprintf("scan %T", tbl))
	}
}
