package page

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

const (
	// Size is the fix size of a page, which is 16KB or 16384 bytes.
	Size = 1 << 14
	// HeaderSize is the fixed size of a page's header.
	HeaderSize = 1 << 5
	// BodySize is the fixed size of a page's body.
	BodySize = Size - HeaderSize
)

// Header field offset in page data.
const (
	idOffset             = 0 // byte 1,2,3,4: byte page ID
	cellCountOffset      = 4 // byte 5,6: cell count
	firstFreeBlockOffset = 6 // byte 7, 8: first free block
	overflowOffset       = 8 // byte 7, 8, 9, 10: overflow page ID
)

var (
	byteOrder = binary.BigEndian
)

// Page is a page implementation that does not support overflow pages. It is not
// meant for that. Since we want to separate index and data, records should not
// contain datasets, but rather enough information, to find the corresponding
// dataset in a data file.
type Page struct {
	// data is the complete data of this page, which consists of header +
	// content. The length of the data is page.Size.
	data []byte
	// header is the header data. The length of the header is page.HeaderSize.
	header []byte
	// body is the actual body of the page, excluding the header. The length of
	// the body is page.BodySize.
	body []byte

	// dirty indicates whether the page has been modified since the last time it
	// has been synchronized with secondary storage.
	dirty bool
}

// New creates a new page with the given ID.
func New(id ID) (*Page, error) {
	data := make([]byte, Size)
	byteOrder.PutUint32(data[idOffset:], id)
	return Load(data)
}

// Load loads the given data into the page. The length of the given data byte
// slice may differ from v1.PageSize, however, it cannot exceed ^uint16(0)-1
// (65535 or 64KB), and must be larger than 22 (HeaderSize(=10) + 1 Offset(=4) +
// 1 empty cell(=8)).
func Load(data []byte) (*Page, error) {
	return load(data)
}

// ID returns the ID of this page. This value must be constant.
func (p *Page) ID() ID { return DecodeID(p.header[idOffset:]) }

// CellCount returns the amount of stored cells in this page. This value is NOT
// constant.
func (p *Page) CellCount() uint16 { return byteOrder.Uint16(p.header[cellCountOffset:]) }

// Dirty returns whether the page is dirty (needs syncing with secondary
// storage).
func (p *Page) Dirty() bool { return p.dirty }

// MarkDirty marks this page as dirty.
func (p *Page) MarkDirty() { p.dirty = true }

// ClearDirty marks this page as NOT dirty.
func (p *Page) ClearDirty() { p.dirty = false }

// StorePointerCell stores a pointer cell in this page. A pointer cell points to
// other page IDs.
func (p *Page) StorePointerCell(cell PointerCell) error {
	return p.storePointerCell(cell)
}

// StoreRecordCell stores a record cell in this page. A record cell holds
// arbitrary, variable size data.
func (p *Page) StoreRecordCell(cell RecordCell) error {
	return p.storeRecordCell(cell)
}

// DeleteCell deletes a cell with the given key. If no such cell could be found,
// false is returned. An error at this point would mean a corrupted page. Do not
// use this method to check if a page is corrupted.
func (p *Page) DeleteCell(key []byte) (ok bool, err error) {
	slotIndex, slot, _, found := p.findCell(key)
	if !found {
		return false, nil
	}

	freeBlock := p.freeBlock()
	// delete the cell by overwriting it will cell data from the right
	endOfCellData := slot.Offset + slot.Size // end of cell
	// delete (zero) the page data in case the move and zero doesn't fully overwrite it
	p.zero(slot.Offset, slot.Size)
	p.moveAndZero(endOfCellData, freeBlock.Offset-endOfCellData, slot.Offset)

	// delete the slot
	startOfAllOffsetData := freeBlock.Offset + freeBlock.Size
	startOfOffsetData := startOfAllOffsetData + slotIndex*SlotByteSize
	p.zero(startOfOffsetData, SlotByteSize)
	p.moveAndZero(startOfAllOffsetData, startOfOffsetData-startOfAllOffsetData, startOfAllOffsetData+SlotByteSize)

	// update header information
	p.setFreeBlockStart(freeBlock.Offset - slot.Size)
	p.decrementCellCount(1)
	p.MarkDirty()

	return true, nil
}

// Cell returns a cell from this page with the given key, or false if no such
// cell exists in this page. In that case, the returned page is also nil.
func (p *Page) Cell(key []byte) (CellTyper, bool) {
	_, _, cell, found := p.findCell(key)
	return cell, found
}

// Cells decodes all cells in this page, which can be expensive, and returns all
// of them. The returned cells do not point back to the original page data, so
// don't modify them. Instead, delete the old cell and store a new one.
func (p *Page) Cells() (result []CellTyper) {
	for _, offset := range p.OccupiedSlots() {
		result = append(result, decodeCell(p.data[offset.Offset:offset.Offset+offset.Size]))
	}
	return
}

// OccupiedSlots returns all occupied slots in the page. The slots all point to
// cells in the page. The amount of slots will always be equal to the amount of
// cells stored in a page. The amount of slots in the page depends on the cell
// count of this page, not the other way around.
func (p *Page) OccupiedSlots() (result []Slot) {
	cellCount := p.CellCount()
	offsetsWidth := cellCount * SlotByteSize
	offsetData := p.body[BodySize-offsetsWidth:]
	for i := uint16(0); i < cellCount; i++ {
		result = append(result, decodeOffset(offsetData[i*SlotByteSize:i*SlotByteSize+SlotByteSize]))
	}
	return
}

// CopyOfData returns a full copy of the page's data, including the header and
// the body. This is not safe for concurrent use.
func (p *Page) CopyOfData() []byte {
	cp := make([]byte, len(p.data))
	copy(cp, p.data)
	return cp
}

func load(data []byte) (*Page, error) {
	if len(data) > int(^uint16(0))-1 {
		return nil, fmt.Errorf("page size too large: %v (max %v)", len(data), int(^uint16(0))-1)
	}
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("page size too small: %v (min %v)", len(data), HeaderSize)
	}

	return &Page{
		data:   data,
		header: data[:HeaderSize],
		body:   data[HeaderSize:],
	}, nil
}

// findCell searches for a cell with the given key, as well as the corresponding
// slot and the corresponding slot index. The index is the index of the cell
// slot in all slots, meaning that the byte location of the offset in the
// page can be obtained with slotIndex*SlotByteSize. The cellSlot is the
// slot that points to the cell. cell is the cell that was found, or nil if no
// cell with the given key could be found. If no cell could be found,
// found=false will be returned, as well as zero values for all other return
// arguments.
func (p *Page) findCell(key []byte) (slotIndex uint16, cellSlot Slot, cell CellTyper, found bool) {
	offsets := p.OccupiedSlots()
	result := sort.Search(len(offsets), func(i int) bool {
		cell := p.cellAt(offsets[i])
		switch c := cell.(type) {
		case RecordCell:
			return bytes.Compare(c.Key, key) >= 0
		case PointerCell:
			return bytes.Compare(c.Key, key) >= 0
		}
		return false
	})
	if result == len(offsets) {
		return 0, Slot{}, nil, false
	}
	return uint16(result), offsets[result], p.cellAt(offsets[result]), true
}

func (p *Page) storePointerCell(cell PointerCell) error {
	return p.storeRawCell(encodePointerCell(cell))
}

func (p *Page) storeRecordCell(cell RecordCell) error {
	return p.storeRawCell(encodeRecordCell(cell))
}

func (p *Page) overflowPageID() (ID, bool) {
	id := byteOrder.Uint32(p.header[overflowOffset:])
	return id, id != 0
}

// freeBlock returns the free block of this page. If the size of the returned
// slot is negative or zero, that means that the page has no more space left.
func (p *Page) freeBlock() Slot {
	freeBlock := byteOrder.Uint16(p.header[firstFreeBlockOffset:])
	return Slot{
		Offset: freeBlock,
		Size:   BodySize - (p.CellCount() * SlotByteSize) - freeBlock,
	}
}

func (p *Page) setFreeBlockStart(newStart uint16) {
	byteOrder.PutUint16(p.header[firstFreeBlockOffset:], newStart)
}

func (p *Page) storeRawCell(rawCell []byte) error {
	size := uint16(len(rawCell))
	slot := p.freeBlock()
	if size > slot.Size {
		return ErrPageFull
	}
	copy(p.body[slot.Offset:], rawCell)
	p.storeCellSlot(Slot{
		Offset: slot.Offset,
		Size:   size,
	})
	p.incrementCellCount(1)
	p.setFreeBlockStart(slot.Offset + size)
	p.MarkDirty()
	return nil
}

// storeCellSlot inserts (sorted) the given slot into all slots at the end of this page.
// This assumes, that the cell data has already been inserted into the page at the given
// slot.
func (p *Page) storeCellSlot(slot Slot) {
	offsets := append(p.OccupiedSlots(), slot)
	sort.Slice(offsets, func(i, j int) bool {
		var leftKey, rightKey []byte
		switch c := p.cellAt(offsets[i]).(type) {
		case RecordCell:
			leftKey = c.Key
		case PointerCell:
			leftKey = c.Key
		}
		switch c := p.cellAt(offsets[j]).(type) {
		case RecordCell:
			rightKey = c.Key
		case PointerCell:
			rightKey = c.Key
		}
		return bytes.Compare(leftKey, rightKey) >= 0
	})
	for i, offset := range offsets {
		offset.encodeInto(p.body[BodySize-(i+1)*int(SlotByteSize):])
	}
}

func (p *Page) cellAt(slot Slot) CellTyper {
	return decodeCell(p.body[slot.Offset : slot.Offset+slot.Size])
}

// moveAndZero moves target bytes in the page's raw data from offset to target,
// and zeros all bytes from offset to offset+size, that do not overlap with the
// target area. Source and target area may overlap.
//
//  [1,1,2,2,2,1,1,1,1,1]
//  moveAndZero(2, 3, 6)
//  [1,1,0,0,0,1,2,2,2,1]
//
// or, with overlap
//
//  [1,1,2,2,2,1,1,1,1,1]
//  moveAndZero(2, 3, 4)
//  [1,1,0,0,2,2,2,1,1,1]
func (p *Page) moveAndZero(offset, size, target uint16) {
	if target == offset {
		// no-op when offset and target are the same
		return
	}

	_ = p.data[offset+size-1] // bounds check
	_ = p.data[target+size-1] // bounds check

	copy(p.data[target:target+size], p.data[offset:offset+size])

	// area needs zeroing
	if target > offset+size || target+size < offset {
		// no overlap
		p.zero(offset, size)
	} else {
		// overlap
		if target > offset && target <= offset+size {
			// move to right, zero non-overlapping area
			p.zero(offset, target-offset)
		} else if target < offset && target+size >= offset {
			// move to left, zero non-overlapping area
			p.zero(target+size, offset-target)
		}
	}
}

// zero zeroes size bytes, starting at offset in the page's raw data.
func (p *Page) zero(offset, size uint16) {
	for i := uint16(0); i < size; i++ {
		p.data[offset+i] = 0
	}
}

func (p *Page) incrementCellCount(delta uint16) { p.incrementUint16(cellCountOffset, delta) }
func (p *Page) decrementCellCount(delta uint16) { p.decrementUint16(cellCountOffset, delta) }

func (p *Page) storeUint16(at, val uint16)  { byteOrder.PutUint16(p.data[at:], val) }
func (p *Page) loadUint16(at uint16) uint16 { return byteOrder.Uint16(p.data[at:]) }

func (p *Page) incrementUint16(at, delta uint16) { p.storeUint16(at, p.loadUint16(at)+delta) }
func (p *Page) decrementUint16(at, delta uint16) { p.storeUint16(at, p.loadUint16(at)-delta) }
