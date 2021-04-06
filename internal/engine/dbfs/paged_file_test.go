package dbfs

import (
	"github.com/spf13/afero"

	"github.com/xqueries/xdb/internal/engine/page"
)

func (suite *DBFSSuite) TestPagedFile() {
	fs := afero.NewMemMapFs()
	f, err := fs.Create("test")
	suite.NoError(err)

	pf, err := newPagedFile(f)
	suite.NoError(err)
	suite.FileEmpty(fs, f.Name())

	suite.Equal(0, pf.PageCount())

	for i := 0; i < 100; i++ {
		p, err := pf.AllocateNewPage()
		suite.NoError(err)
		suite.EqualValues(page.ID(i), p.ID())
	}

	suite.Equal(100, pf.PageCount())

	fileInfo, err := f.Stat()
	suite.NoError(err)
	suite.EqualValues(page.Size*pf.PageCount(), fileInfo.Size())
}
