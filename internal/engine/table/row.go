package table

import (
	"github.com/xqueries/xdb/internal/engine/types"
)

// Row is a collection of values with no col information. To have col
// information available for the values, see table.RowWithColInfo.
type Row struct {
	Values []types.Value
}
