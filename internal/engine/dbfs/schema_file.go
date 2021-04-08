package dbfs

import (
	"fmt"
	"io"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"

	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

type SchemaFile struct {
	file         afero.File
	highestRowID int
	columns      []table.Col
}

type schemaYaml struct {
	HighestRowID int          `yaml:"highest_row_id"`
	Columns      []columnYaml `yaml:"columns"`
}

type columnYaml struct {
	QualifiedName string              `yaml:"qualified_name"`
	Alias         string              `yaml:"alias"`
	Type          types.TypeIndicator `yaml:"type"`
}

func newSchemaFile(file afero.File) (*SchemaFile, error) {
	sch := &SchemaFile{
		file:         file,
		highestRowID: -1,
	}
	if err := sch.load(); err != nil {
		return nil, fmt.Errorf("load schema: %w", err)
	}
	return sch, nil
}

func (sf *SchemaFile) load() error {
	dec := yaml.NewDecoder(sf.file)

	var syaml schemaYaml
	if err := dec.Decode(&syaml); err != nil {
		if err == io.EOF {
			// probably no content yet
			return nil
		}
		return fmt.Errorf("decode: %w", err)
	}

	sf.highestRowID = syaml.HighestRowID
	sf.columns = nil
	for _, column := range syaml.Columns {
		sf.columns = append(sf.columns, table.Col{
			QualifiedName: column.QualifiedName,
			Alias:         column.Alias,
			Type:          types.ByIndicator(column.Type),
		})
	}

	return nil
}

func (sf *SchemaFile) HighestRowID() int {
	return sf.highestRowID
}

func (sf *SchemaFile) IncrementHighestRowID() {
	sf.highestRowID++
}

func (sf *SchemaFile) Columns() []table.Col {
	return sf.columns
}

func (sf *SchemaFile) SetColumns(cols []table.Col) {
	sf.columns = cols
}

// Store writes the schema file back to disk.
func (sf *SchemaFile) Store() error {
	if err := sf.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate(0): %w", err)
	}
	if _, err := sf.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek: %w", err)
	}

	var syaml schemaYaml
	syaml.HighestRowID = sf.highestRowID
	for _, column := range sf.columns {
		syaml.Columns = append(syaml.Columns, columnYaml{
			QualifiedName: column.QualifiedName,
			Alias:         column.Alias,
			Type:          types.IndicatorFor(column.Type),
		})
	}

	enc := yaml.NewEncoder(sf.file)
	if err := enc.Encode(&syaml); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return nil
}

func (sf *SchemaFile) Close() error {
	return sf.file.Close()
}
