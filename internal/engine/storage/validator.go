package storage

import (
	"fmt"
	"os"

	"github.com/spf13/afero"
	"github.com/xqueries/xdb/internal/engine/storage/page"
)

// Validator can be used to validate a database file prior to opening it. If
// validation fails, a speaking error is returned. If validation does not fail,
// the file is a valid database file and can be used. Valid means, that the file
// is not structurally corrupted and usable.
type Validator struct {
	file afero.File
	info os.FileInfo
	mgr  *PageManager
}

// NewValidator creates a new validator over the given file.
func NewValidator(file afero.File) *Validator {
	return &Validator{
		file: file,
		// info, mgr is set on every run of Validate()
	}
}

// Validate runs the file validation and returns a speaking error on why the
// validation failed, if it failed.
func (v *Validator) Validate() error {
	stat, err := v.file.Stat()
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	v.info = stat
	mgr, err := NewPageManager(v.file)
	if err != nil {
		return fmt.Errorf("new page manager: %w", err)
	}
	v.mgr = mgr

	validations := []struct {
		name      string
		validator func() error
	}{
		{"is file", v.validateIsFile},
		{"size", v.validateSize},
		{"page 0", v.validatePage0},
	}

	for _, validation := range validations {
		if err := validation.validator(); err != nil {
			return fmt.Errorf("%v: %w", validation.name, err)
		}
	}

	return nil
}

func (v Validator) validateIsFile() error {
	if v.info.IsDir() {
		return fmt.Errorf("file is directory")
	}
	if !v.info.Mode().Perm().IsRegular() {
		return fmt.Errorf("file is not a regular file")
	}
	return nil
}

func (v Validator) validateSize() error {
	size := v.info.Size()
	if size%page.Size != 0 {
		return fmt.Errorf("invalid file size, must be multiple of page size (=%v), but was %v", page.Size, size)
	}
	return nil
}

func (v Validator) validatePage0() error {
	idBuf := make([]byte, 4)
	_, err := v.file.ReadAt(idBuf, 0)
	if err != nil {
		return fmt.Errorf("read at 0: %w", err)
	}
	if idBuf[0] != 0 ||
		idBuf[1] != 0 ||
		idBuf[2] != 0 ||
		idBuf[3] != 0 {
		return fmt.Errorf("ID of page at offset 0 is not 0")
	}

	page0, err := v.mgr.ReadPage(HeaderPageID)
	if err != nil {
		return fmt.Errorf("read Page 0: %w", err)
	}
	if page0.CellCount() != 2 {
		return fmt.Errorf("page 0 must have exactly 2 cells")
	}
	return nil
}
