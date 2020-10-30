package engine

import (
	"fmt"

	"github.com/xqueries/xdb/internal/compiler/command"
	"github.com/xqueries/xdb/internal/engine/table"
)

func (e Engine) evaluateProjection(ctx ExecutionContext, proj command.Project) (table.Table, error) {
	defer e.profiler.Enter("projection").Exit()

	origin, err := e.evaluateList(ctx, proj.Input)
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}

	if len(proj.Cols) == 0 {
		e.log.Debug().
			Msg("projection filters all projectedColumns")
		return table.Empty, nil
	}

	tbl, err := e.newProjectedTable(ctx, origin, proj.Cols)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}
