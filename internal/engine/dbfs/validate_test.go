package dbfs

import (
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	require := require.New(t)

	mkdir := func(fs afero.Fs, paths ...string) {
		require.NoError(fs.Mkdir(filepath.Join(paths...), defaultDirPerm))
	}
	write := func(fs afero.Fs, content string, paths ...string) {
		require.NoError(afero.WriteFile(fs, filepath.Join(paths...), []byte(content), defaultFilePerm))
	}

	/*
		TESTCASES
	*/
	testcases := []struct {
		name    string
		setup   func(afero.Fs)
		isValid bool
	}{
		{
			"minimal",
			func(fs afero.Fs) {
				mkdir(fs, TablesDirectory)
				write(fs, `
count: 1
tables:
  myTable: somesampleID
`, TablesDirectory, TablesInfoFile)
				mkdir(fs, TablesDirectory, "somesampleID")
				write(fs, "", TablesDirectory, "somesampleID", TableDataFile)
				write(fs, "", TablesDirectory, "somesampleID", TableSchemaFile)
			},
			true,
		},
		{
			"mismatched count no tables",
			func(fs afero.Fs) {
				mkdir(fs, TablesDirectory)
				write(fs, `count: 1`, TablesDirectory, TablesInfoFile)
				mkdir(fs, TablesDirectory, "somesampleID")
				write(fs, "", TablesDirectory, "somesampleID", TableDataFile)
				write(fs, "", TablesDirectory, "somesampleID", TableSchemaFile)
			},
			false,
		},
		{
			"no count in tables.info",
			func(fs afero.Fs) {
				mkdir(fs, TablesDirectory)
				write(fs, `
tables:
  myTable: somesampleID
`, TablesDirectory, TablesInfoFile)
				mkdir(fs, TablesDirectory, "somesampleID")
				write(fs, "", TablesDirectory, "somesampleID", TableDataFile)
				write(fs, "", TablesDirectory, "somesampleID", TableSchemaFile)
			},
			false,
		},
		{
			"mismatched count 5",
			func(fs afero.Fs) {
				mkdir(fs, TablesDirectory)
				write(fs, `
count: 5
tables:
  myTable: somesampleID
`, TablesDirectory, TablesInfoFile)
				mkdir(fs, TablesDirectory, "somesampleID")
				write(fs, "", TablesDirectory, "somesampleID", TableDataFile)
				write(fs, "", TablesDirectory, "somesampleID", TableSchemaFile)
			},
			false,
		},
		{
			"mismatched count 0",
			func(fs afero.Fs) {
				mkdir(fs, TablesDirectory)
				write(fs, `
count: 0
tables:
  myTable: somesampleID
`, TablesDirectory, TablesInfoFile)
				mkdir(fs, TablesDirectory, "somesampleID")
				write(fs, "", TablesDirectory, "somesampleID", TableDataFile)
				write(fs, "", TablesDirectory, "somesampleID", TableSchemaFile)
			},
			false,
		},
		{
			"missing table directory",
			func(fs afero.Fs) {
				mkdir(fs, TablesDirectory)
				write(fs, `
count: 1
tables:
  myTable: somesampleID
`, TablesDirectory, TablesInfoFile)
			},
			false,
		},
		{
			"no tables.info file",
			func(fs afero.Fs) {
				mkdir(fs, TablesDirectory)
				mkdir(fs, TablesDirectory, "somesampleID")
				write(fs, "", TablesDirectory, "somesampleID", TableDataFile)
				write(fs, "", TablesDirectory, "somesampleID", TableSchemaFile)
			},
			false,
		},
		{
			"no data file",
			func(fs afero.Fs) {
				mkdir(fs, TablesDirectory)
				write(fs, `
count: 1
tables:
  myTable: somesampleID
`, TablesDirectory, TablesInfoFile)
				mkdir(fs, TablesDirectory, "somesampleID")
				write(fs, "", TablesDirectory, "somesampleID", TableSchemaFile)
			},
			false,
		},
		{
			"no schema file",
			func(fs afero.Fs) {
				mkdir(fs, TablesDirectory)
				write(fs, `
count: 1
tables:
  myTable: somesampleID
`, TablesDirectory, TablesInfoFile)
				mkdir(fs, TablesDirectory, "somesampleID")
				write(fs, "", TablesDirectory, "somesampleID", TableDataFile)
			},
			false,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			tt.setup(fs)

			assert := assert.New(t)

			err := Validate(fs)
			if err != nil {
				if tt.isValid {
					assert.NoError(err)
				} else {
					assert.Error(err)
				}
			} else {
				if !tt.isValid {
					// must be invalid but got no error
					assert.FailNow("expected an error, but got none")
				}
				// nothing to do here
			}
		})
	}
}
