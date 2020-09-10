package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xqueries/xdb/internal/compiler/command"
)

func TestFullTableScan(t *testing.T) {
	t.Skip("full table scan not implemented yet")

	assert := assert.New(t)

	e := createEngineOnEmptyDatabase(t)
	result, err := e.Evaluate(command.Scan{
		Table: command.SimpleTable{
			Table: "myTable",
		},
	})
	assert.NoError(err)
	assert.Nil(result)
}
