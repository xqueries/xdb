package dbfs

import (
	"fmt"
	"io"

	"gopkg.in/yaml.v3"

	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

// SchemaFile provides access to the schema of a table.
// After modification, call Store to write the modified
// schema back to disk.
type SchemaFile struct {
	HighestRowID int
	Columns      []table.Col
}

// schemaYaml is an intermediate structure used for encoding
// a SchemaFile into yaml.
type schemaYaml struct {
	HighestRowID int          `yaml:"highest_row_id"`
	Columns      []columnYaml `yaml:"columns"`
}

// columnYaml is an intermediate structure used for encoding
// a table.Col into yaml.
type columnYaml struct {
	QualifiedName string              `yaml:"qualified_name"`
	Alias         string              `yaml:"alias"`
	Type          types.TypeIndicator `yaml:"type"`
}

func (sf *SchemaFile) load(rd io.Reader) error {
	dec := yaml.NewDecoder(rd)

	var syaml schemaYaml
	if err := dec.Decode(&syaml); err != nil {
		if err == io.EOF {
			// probably no content yet
			return nil
		}
		return fmt.Errorf("decode: %w", err)
	}

	sf.HighestRowID = syaml.HighestRowID
	sf.Columns = nil
	for _, column := range syaml.Columns {
		sf.Columns = append(sf.Columns, table.Col{
			QualifiedName: column.QualifiedName,
			Alias:         column.Alias,
			Type:          types.ByIndicator(column.Type),
		})
	}

	return nil
}
