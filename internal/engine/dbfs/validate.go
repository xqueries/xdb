package dbfs

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/afero"
)

type custom func() error

func (custom) path() string { return "" }

type dir []string

func (d dir) path() string { return filepath.Join(d...) }

type file []string

func (f file) path() string { return filepath.Join(f...) }

type elem interface {
	path() string
}

// Validate checks a file system for a valid database file structure.
// This check is performed in the root directory of the given file system.
// If an error occurs, it will be returned.
//
// Note: a file structure is also considered invalid, if the contents of certain
// files do not match. Calling this method will not check every file for its
// contents, especially no data files. However, it will check for integrity of the
// tables.info file any will check any indices, if present.
func Validate(fs afero.Fs) error {
	dbfs := &DBFS{
		fs: fs,
	}

	check := func(checks ...elem) error {
		for _, check := range checks {
			p := check.path()
			if exists, err := afero.Exists(fs, p); err != nil {
				return fmt.Errorf("exists: %w", err)
			} else if !exists {
				return fmt.Errorf("'%s' does not exist", p)
			}
			switch checkT := check.(type) {
			case custom:
				if err := checkT(); err != nil {
					return err
				}
			case dir:
				if isDir, err := afero.IsDir(fs, p); err != nil {
					return fmt.Errorf("isDir: %w", err)
				} else if !isDir {
					return fmt.Errorf("'%s' is not a directory", p)
				}
			case file:
				if isDir, err := afero.IsDir(fs, p); err != nil {
					return fmt.Errorf("isDir: %w", err)
				} else if isDir {
					return fmt.Errorf("'%s' is a directory, not a file", p)
				}
			}
		}
		return nil
	}

	if err := check(
		dir{TablesDirectory},
		file{TablesDirectory, TablesInfoFile},
		custom(func() error {
			// tables.info is available, check contents
			tableInfos, err := dbfs.loadTablesInfo()
			if err != nil {
				return fmt.Errorf("load tables info: %w", err)
			}
			if tableInfos.Count != len(tableInfos.Tables) {
				return fmt.Errorf("tables.info>count (%v) does not match length of tables.info>tables (%v)", tableInfos.Count, len(tableInfos.Tables))
			}

			// for each table that is defined in tables.info
			for _, v := range tableInfos.Tables {
				// check if all files and directories exist
				if err = check(
					dir{TablesDirectory, v},
					file{TablesDirectory, v, TableDataFile},
					file{TablesDirectory, v, TableSchemaFile},
				); err != nil {
					return err
				}
			}
			return nil
		}),
	); err != nil {
		return err
	}

	return nil
}
