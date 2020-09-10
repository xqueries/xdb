package engine

import (
	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/id"
)

// ExecutionContext is a context that is passed down throughout a complete
// evaluation. It may be populated further.
type ExecutionContext struct {
	id id.ID

	intermediateRow table.RowWithColInfo
}

func newEmptyExecutionContext() ExecutionContext {
	return ExecutionContext{
		id: id.Create(),
	}
}

// IntermediateRow sets the current intermediate row that might be needed for evaluating expressions
// in the context, and returns the context.
func (c ExecutionContext) IntermediateRow(row table.RowWithColInfo) ExecutionContext {
	c.intermediateRow = row
	return c
}

func (c ExecutionContext) String() string {
	return c.id.String()
}
