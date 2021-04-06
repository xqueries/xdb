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
	f, err := t.fs.Open(TableDataFile)
	if err != nil {
		return nil, fmt.Errorf("open '%s/%s': %w", t.id.String(), TableDataFile, err)
	}

	pf, err := newPagedFile(f)
	if err != nil {
		return nil, fmt.Errorf("load paged file '%s/%s': %w", t.id.String(), TableDataFile, err)
	}

	return pf, nil
}

// SchemaFile returns the file which stores the schema information of this database.
func (t Table) SchemaFile() (afero.File, error) {
	f, err := t.fs.OpenFile(TableSchemaFile, os.O_RDWR, defaultPerm)
	if err != nil {
		return nil, fmt.Errorf("open '%s/%s': %w", t.id.String(), TableSchemaFile, err)
	}
	return f, nil
}
