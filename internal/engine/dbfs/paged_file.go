package dbfs

import (
	"fmt"

	"github.com/spf13/afero"

	"github.com/xqueries/xdb/internal/engine/page"
)

// PagedFile provides access to pages within a file that is made up
// of pages. Everything about this file is expensive, and pages and objects
// of this type should probably be cached whenever possible.
type PagedFile struct {
	file        afero.File
	offsetIndex map[page.ID]int64
	fileSize    int64
}

func newPagedFile(file afero.File) (*PagedFile, error) {
	pf := &PagedFile{
		file:        file,
		offsetIndex: make(map[page.ID]int64),
	}
	if err := pf.initOffsetIndex(); err != nil {
		return nil, err
	}
	return pf, nil
}

func (pf *PagedFile) initOffsetIndex() error {
	fileInfo, err := pf.file.Stat()
	if err != nil {
		return fmt.Errorf("stat '%s': %w", pf.file.Name(), err)
	}
	size := fileInfo.Size()
	if size%page.Size != 0 {
		return fmt.Errorf("file has size %v, which is not divisible by the size of a single page", size)
	}
	pf.fileSize = size

	idBuf := make([]byte, page.IDSize)
	for i := int64(0); i < size; i += page.Size {
		_, err := pf.file.ReadAt(idBuf, i)
		if err != nil {
			return fmt.Errorf("read at %v: %w", i, err)
		}
		id := page.DecodeID(idBuf)
		pf.offsetIndex[id] = i
	}
	return nil
}

// Pages returns a slice of page IDs that are present in this paged file.
// The returned slice is not sorted.
func (pf *PagedFile) Pages() []page.ID {
	res := make([]page.ID, 0, len(pf.offsetIndex))
	for k := range pf.offsetIndex {
		res = append(res, k)
	}
	return res
}

// PageCount returns the amount of pages that exist in this file.
func (pf *PagedFile) PageCount() int {
	return len(pf.offsetIndex)
}

// LoadPage loads a page with the given ID from the disk. If no such page exists,
// an error is returned. The returned page is an in-memory copy of the page on disk,
// and modifying will not change any data on the disk. To modify disk data,
// modify the page and call StorePage with the modified page.
func (pf *PagedFile) LoadPage(id page.ID) (*page.Page, error) {
	offset, ok := pf.offsetIndex[id]
	if !ok {
		return nil, ErrPageNotExist
	}

	buf := make([]byte, page.Size)
	_, err := pf.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("read page %v: %w", id, err)
	}

	p, err := page.Load(buf)
	if err != nil {
		return nil, fmt.Errorf("load page %v: %w", id, err)
	}
	return p, nil
}

// StorePage stores the contents of the page at the offset associated with the page.ID.
// If there is no offset associated with the page.ID, an error will be returned.
func (pf *PagedFile) StorePage(p *page.Page) error {
	id := p.ID()
	offset, ok := pf.offsetIndex[id]
	if !ok {
		return ErrPageNotExist
	}

	data := p.CopyOfData()
	_, err := pf.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("write at: %w", err)
	}
	return nil
}

// AllocateNewPage will create a new page in this file and immediately
// write it to disk. The ID of the new page will be determined by calling
// FindUnusedPageID.
func (pf *PagedFile) AllocateNewPage() (*page.Page, error) {
	newID, err := pf.FindUnusedPageID()
	if err != nil {
		return nil, fmt.Errorf("find unused page ID: %w", err)
	}

	return pf.AllocatePageWithID(newID)
}

// AllocatePageWithID will allocate a new page, but instead of finding
// an available page ID, this will use the given ID. If a page with that ID
// already exists, an error is returned.
// Otherwise, the page is added to the PagedFile and a full page is allocated
// on disk.
func (pf *PagedFile) AllocatePageWithID(id page.ID) (*page.Page, error) {
	if _, ok := pf.offsetIndex[id]; ok {
		return nil, fmt.Errorf("page already exists")
	}

	newPage, err := page.New(id)
	if err != nil {
		return nil, fmt.Errorf("new page: %w", err)
	}
	pageOffset := pf.fileSize       // offset of the page in the file
	pf.offsetIndex[id] = pageOffset // remember the offset
	pf.fileSize += page.Size        // update the file size by adding the size of one full page

	// store the new page. we can do this, since the new page
	// already has an offset in the offset index.
	if err := pf.StorePage(newPage); err != nil {
		return nil, fmt.Errorf("store new page: %w", err)
	}
	return newPage, nil
}

// FindUnusedPageID finds an unused page ID in this paged file.
// You can use this together with AllocatePageWithID, or use
// AllocateNewPage in the first place.
func (pf *PagedFile) FindUnusedPageID() (page.ID, error) {
	for i := page.ID(0); ; i++ {
		if _, ok := pf.offsetIndex[i]; !ok {
			return i, nil
		}
	}
}

// Close will close the underlying file.
func (pf *PagedFile) Close() error {
	return pf.file.Close()
}
