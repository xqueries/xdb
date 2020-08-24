package table

import "github.com/xqueries/xdb/internal/engine/types"

// Col is a header for a single column in a table, containing the qualified name
// of the col, a possible alias and the col data type.
type Col struct {
	QualifiedName string
	Alias         string
	Type          types.Type
}

func (c Col) String() string {
	result := c.QualifiedName + " ("
	if c.Alias != "" {
		result += "alias " + c.Alias + ", "
	}
	result += "type " + c.Type.String()
	return result
}
