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
	case command.CreateTable:
		tbl, err := e.evaluateCreateTable(ctx, cmd)
		if err != nil {
			return nil, fmt.Errorf("create table: %w", err)
		}
		return tbl, nil
	case command.Insert:
		tbl, err := e.evaluateInsert(ctx, cmd)
		if err != nil {
			return nil, fmt.Errorf("insert into %v: %w", cmd.Table.QualifiedName(), err)
		}
		return tbl, nil
	}
	return nil, ErrUnimplemented(c)
}

func (e Engine) evaluateList(ctx ExecutionContext, l command.List) (table.Table, error) {
	switch list := l.(type) {
	case command.Values:
		values, err := e.evaluateValues(ctx, list)
		if err != nil {
			return nil, fmt.Errorf("values: %w", err)
		}
		return values, nil
	case command.Scan:
		scanned, err := e.evaluateScan(list)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		return scanned, nil
	case command.Project:
		return e.evaluateProjection(ctx, list)
	case command.Select:
		return e.evaluateSelection(ctx, list)
	}
	return nil, ErrUnimplemented(l)
}
