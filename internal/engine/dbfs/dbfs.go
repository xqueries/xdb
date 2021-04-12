package dbfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"

	"github.com/xqueries/xdb/internal/id"
)

const defaultDirPerm os.FileMode = 0744
const defaultFilePerm os.FileMode = 0644

// DBFS represents the structure of a single DataBase-FileSystem.
// It provides easy access to all folders and files within the root
// directory, with wrappers for easily modifying the files.
type DBFS struct {
	fs afero.Fs
}

// CreateNew initializes a new, empty DBFS in the root of the given file system.
func CreateNew(fs afero.Fs) (*DBFS, error) {
	dbfs := &DBFS{
		fs: fs,
	}

	if err := dbfs.mkdir(TablesDirectory); err != nil {
		return nil, err
	}
	if err := dbfs.touch(filepath.Join(TablesDirectory, TablesInfoFile)); err != nil {
		return nil, err
	}

	if err := dbfs.Close(); err != nil {
		return nil, fmt.Errorf("close stub dbfs: %w", err)
	}
	return Load(fs)
}

// Load loads a DBFS from the given file system.
func Load(fs afero.Fs) (*DBFS, error) {
	if err := Validate(fs); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}
	return &DBFS{
		fs: fs,
	}, nil
}

// Close closes the DBFS and releases all resources.
func (dbfs *DBFS) Close() error {
	// nothing to do here yet
	return nil
}

// TableCount returns the amount of tables that currently exist in this database.
func (dbfs *DBFS) TableCount() (int, error) {
	infos, err := dbfs.loadTablesInfo()
	if err != nil {
		return 0, err
	}

	return infos.Count, nil
}

// Table returns a Table object representing the files and directories
// for the table with the given name, or an error if the table does not
// exist. Call HasTable to see whether a table with a given name exists.
func (dbfs *DBFS) Table(name string) (Table, error) {
	infos, err := dbfs.loadTablesInfo()
	if err != nil {
		return Table{}, err
	}

	// perform checks
	tblID, ok := infos.Tables[name]
	if !ok {
		return Table{}, fmt.Errorf("table '%s' does not exist", name)
	}

	return Table{
		fs: afero.NewBasePathFs(dbfs.fs, filepath.Join(TablesDirectory, tblID)),
	}, nil
}

// HasTable determines whether or not a table with the given name exists in this
// database.
func (dbfs *DBFS) HasTable(name string) (bool, error) {
	infos, err := dbfs.loadTablesInfo()
	if err != nil {
		return false, err
	}

	_, ok := infos.Tables[name]
	return ok, nil
}

// CreateTable creates empty table files for a table with the given name.
// This will return an error if a table with the given name already exists.
// Calling this method will also create an entry in the tables info file.
func (dbfs *DBFS) CreateTable(name string) (Table, error) {
	infos, err := dbfs.loadTablesInfo()
	if err != nil {
		return Table{}, err
	}

	if _, ok := infos.Tables[name]; ok {
		return Table{}, fmt.Errorf("table '%s' already exists", name)
	}

	newTableID := id.Create()
	newTableIDString := newTableID.String()
	infos.Tables[name] = newTableIDString
	infos.Count++
	if err := dbfs.storeTablesInfo(infos); err != nil {
		return Table{}, fmt.Errorf("store table info: %w", err)
	}

	tableDir := filepath.Join(TablesDirectory, newTableIDString)
	if err := dbfs.mkdir(tableDir); err != nil {
		return Table{}, err
	}
	if err := dbfs.touch(filepath.Join(tableDir, TableDataFile)); err != nil {
		return Table{}, err
	}
	if err := dbfs.touch(filepath.Join(tableDir, TableSchemaFile)); err != nil {
		return Table{}, err
	}

	return Table{
		id: newTableID,
		fs: afero.NewBasePathFs(dbfs.fs, tableDir),
	}, nil
}

func (dbfs *DBFS) loadTablesInfo() (TablesInfo, error) {
	// load infos to check whether and where the table exists
	infoFilePath := filepath.Join(TablesDirectory, TablesInfoFile)
	infoFile, err := dbfs.fs.Open(infoFilePath)
	if err != nil {
		return TablesInfo{}, fmt.Errorf("open '%s': %w", infoFilePath, err)
	}
	defer func() {
		_ = infoFile.Close()
	}()

	var infos TablesInfo
	infos.Tables = make(map[string]string)
	if err := yaml.NewDecoder(infoFile).Decode(&infos); err != nil && err != io.EOF {
		return TablesInfo{}, fmt.Errorf("decode infos: %w", err)
	}
	return infos, nil
}

func (dbfs *DBFS) storeTablesInfo(info TablesInfo) error {
	infoFilePath := filepath.Join(TablesDirectory, TablesInfoFile)
	infoFile, err := dbfs.fs.OpenFile(infoFilePath, os.O_RDWR, defaultFilePerm)
	if err != nil {
		return fmt.Errorf("open '%s': %w", infoFilePath, err)
	}
	defer func() {
		_ = infoFile.Close()
	}()

	if err := yaml.NewEncoder(infoFile).Encode(&info); err != nil {
		return fmt.Errorf("encode infos: %w", err)
	}
	return nil
}

// touch creates an empty file with the given path in this DBFS.
func (dbfs *DBFS) touch(path string) error {
	f, err := dbfs.fs.OpenFile(path, os.O_CREATE, defaultFilePerm)
	if err != nil {
		return fmt.Errorf("create '%s': %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close '%s': %w", path, err)
	}
	return nil
}

// mkdir creates an empty directory with the given path in this DBFS.
// Parent directories must exist beforehand.
func (dbfs *DBFS) mkdir(path string) error {
	if err := dbfs.fs.Mkdir(path, defaultDirPerm); err != nil {
		return fmt.Errorf("mkdir '%s': %w", path, err)
	}
	return nil
}
