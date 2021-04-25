package dbfs

import (
	"fmt"
	"os"

	"github.com/spf13/afero"

	"github.com/xqueries/xdb/internal/id"
)

// Table is a wrapper around the table directory of a specific table.
type Table struct {
	id id.ID
	fs afero.Fs
}

// DataFile returns the data file of the table that this object represents.
// Calling this will load a new PagedFile, which is expensive. Cache the returned page
// if possible.
func (t Table) DataFile() (*PagedFile, error) {
	f, err := t.fs.OpenFile(TableDataFile, os.O_RDWR, defaultFilePerm)
	if err != nil {
		return nil, fmt.Errorf("open '%s/%s': %w", t.id.String(), TableDataFile, err)
	}

	pf, err := newPagedFile(f)
	if err != nil {
		return nil, fmt.Errorf("load paged file '%s/%s': %w", t.id.String(), TableDataFile, err)
	}

	return pf, nil
}

// SchemaFile returns a new schema file which contains information about the table schema.
func (t Table) SchemaFile() (*SchemaFile, error) {
	f, err := t.fs.OpenFile(TableSchemaFile, os.O_RDWR, defaultFilePerm)
	if err != nil {
		return nil, fmt.Errorf("open '%s/%s': %w", t.id.String(), TableSchemaFile, err)
	}

	sf, err := newSchemaFile(f)
	if err != nil {
		return nil, fmt.Errorf("load schema file '%s/%s': %w", t.id.String(), TableSchemaFile, err)
	}

	return sf, nil
}
