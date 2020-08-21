package page

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func TestPageSuite(t *testing.T) {
	suite.Run(t, new(PageSuite))
}

// PageSuite is a test suite that holds a page for use of every test. Before
// every run, the page will be set up completely new, with a page ID of 1 and no
// cells. The page is not dirty.
type PageSuite struct {
	suite.Suite

	// page is freshly set up for every test with ID=1 and not dirty.
	page *Page
}

func (suite *PageSuite) SetupTest() {
	p, err := New(1)
	suite.NoError(err)

	suite.EqualValues(1, p.ID())
	suite.False(p.Dirty())

	suite.page = p
}

func (suite *PageSuite) TestPage_StoreRecordCell() {
	c := RecordCell{
		Key:    []byte{0xAB},
		Record: []byte{0xCA, 0xFE, 0xBA, 0xBE},
	}

	err := suite.page.StoreRecordCell(c)
	suite.NoError(err)
	suite.EqualValues(1, suite.page.CellCount())
	suite.Equal(Slot{
		Offset: 14, // first free byte in slot
		Size:   BodySize - SlotByteSize - 14,
	}, suite.page.freeBlock())

	overflowPageID, ok := suite.page.overflowPageID()
	suite.False(ok) // page must not have an overflow page allocated
	suite.EqualValues(0, overflowPageID)

	gotCell, ok := suite.page.Cell(c.Key)
	suite.True(ok)
	suite.Equal(c, gotCell)

	suite.True(suite.page.Dirty())
}

func (suite *PageSuite) TestPage_StoreRecordCell_Multiple() {
	cells := []RecordCell{
		{
			Key:    []byte{0x11},
			Record: []byte{0xCA, 0xFE, 0xBA, 0xBE},
		},
		{
			Key:    []byte{0x33},
			Record: []byte{0xD1, 0xCE},
		},
		{
			Key:    []byte{0x22},
			Record: []byte{0xFF},
		},
	}
	suite.NoError(suite.page.StoreRecordCell(cells[0]))
	suite.NoError(suite.page.StoreRecordCell(cells[1]))
	suite.NoError(suite.page.StoreRecordCell(cells[2]))

	suite.True(suite.page.Dirty())

	var ct CellTyper
	var ok bool
	ct, ok = suite.page.Cell(cells[0].Key)
	suite.True(ok)
	suite.Equal(cells[0], ct)
	ct, ok = suite.page.Cell(cells[1].Key)
	suite.True(ok)
	suite.Equal(cells[1], ct)
	ct, ok = suite.page.Cell(cells[2].Key)
	suite.True(ok)
	suite.Equal(cells[2], ct)

	cellDataLength := uint16(14 + 12 + 11)
	suite.EqualValues(BodySize-3*SlotByteSize-cellDataLength, suite.page.freeBlock().Size)
	suite.EqualValues(suite.page.freeBlock().Offset, cellDataLength)
	suite.EqualValues(3, suite.page.CellCount())
}

func (suite *PageSuite) TestPage_DeleteCell() {
	// setup: insert three record cells
	cells := []RecordCell{
		{
			Key:    []byte{0x11},
			Record: []byte{0xCA, 0xFE, 0xBA, 0xBE},
		},
		{
			Key:    []byte{0x33},
			Record: []byte{0xD1, 0xCE},
		},
		{
			Key:    []byte{0x22},
			Record: []byte{0xFF},
		},
	}
	suite.NoError(suite.page.StoreRecordCell(cells[0]))
	suite.NoError(suite.page.StoreRecordCell(cells[1]))
	suite.NoError(suite.page.StoreRecordCell(cells[2]))

	suite.True(suite.page.Dirty())
	suite.page.ClearDirty()
	suite.False(suite.page.Dirty())

	// test: remove the cells
	cellDataLength := uint16(14 + 12 + 11)
	suite.EqualValues(suite.page.freeBlock().Offset, cellDataLength)
	suite.EqualValues(BodySize-3*SlotByteSize-cellDataLength, suite.page.freeBlock().Size)
	suite.EqualValues(3, suite.page.CellCount())

	ok, err := suite.page.DeleteCell(cells[2].Key)
	suite.NoError(err)
	suite.True(ok)
	suite.True(suite.page.Dirty())

	cellDataLength = uint16(14 + 12)
	suite.EqualValues(suite.page.freeBlock().Offset, cellDataLength)
	suite.EqualValues(BodySize-2*SlotByteSize-cellDataLength, suite.page.freeBlock().Size)
	suite.EqualValues(2, suite.page.CellCount())
}

func (suite *PageSuite) TestPage_findCell() {
	cells := []CellTyper{
		PointerCell{
			Key:     []byte("001 first"),
			Pointer: ID(1),
		},
		PointerCell{
			Key:     []byte("002 second"),
			Pointer: ID(2),
		},
		PointerCell{
			Key:     []byte("003 third"),
			Pointer: ID(3),
		},
		PointerCell{
			Key:     []byte("004 fourth"),
			Pointer: ID(4),
		},
	}
	for _, cell := range cells {
		switch c := cell.(type) {
		case RecordCell:
			suite.NoError(suite.page.StoreRecordCell(c))
		case PointerCell:
			suite.NoError(suite.page.StorePointerCell(c))
		default:
			suite.FailNow("unknown cell type")
		}
	}

	// actual tests

	tests := []struct {
		name          string
		key           string
		wantSlotIndex uint16
		wantSlot      Slot
		wantCell      CellTyper
		wantFound     bool
	}{
		{
			name:          "first",
			key:           "001 first",
			wantSlotIndex: 0,
			wantSlot:      Slot{Offset: 0, Size: 18},
			wantCell:      cells[0],
			wantFound:     true,
		},
		{
			name:          "second",
			key:           "002 second",
			wantSlotIndex: 1,
			wantSlot:      Slot{Offset: 18, Size: 19},
			wantCell:      cells[1],
			wantFound:     true,
		},
		{
			name:          "third",
			key:           "003 third",
			wantSlotIndex: 2,
			wantSlot:      Slot{Offset: 37, Size: 18},
			wantCell:      cells[2],
			wantFound:     true,
		},
		{
			name:          "fourth",
			key:           "004 fourth",
			wantSlotIndex: 3,
			wantSlot:      Slot{Offset: 55, Size: 19},
			wantCell:      cells[3],
			wantFound:     true,
		},
		{
			name:          "missing cell",
			key:           "some key that doesn't exist",
			wantSlotIndex: 0,
			wantSlot:      Slot{Offset: 0, Size: 0},
			wantCell:      nil,
			wantFound:     false,
		},
	}
	for _, tt := range tests {
		suite.Run(tt.name, func() {
			slotIndex, slot, cell, found := suite.page.findCell([]byte(tt.key))
			suite.Equal(tt.wantSlotIndex, slotIndex, "slot indices don't match")
			suite.Equal(tt.wantSlot, slot, "cell slot don't match")
			suite.Equal(tt.wantCell, cell, "cell don't match")
			suite.Equal(tt.wantFound, found, "found don't match")
		})
	}
}

func TestPage_moveAndZero(t *testing.T) {
	type args struct {
		offset uint16
		size   uint16
		target uint16
	}
	tests := []struct {
		name string
		data []byte
		args args
		want []byte
	}{
		{
			"same position",
			[]byte{1, 1, 2, 2, 2, 2, 1, 1, 1, 1},
			args{2, 4, 2},
			[]byte{1, 1, 2, 2, 2, 2, 1, 1, 1, 1},
		},
		{
			"single no overlap to right",
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			args{0, 1, 2},
			[]byte{0, 2, 1, 4, 5, 6, 7, 8, 9},
		},
		{
			"double no overlap to right",
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			args{0, 2, 3},
			[]byte{0, 0, 3, 1, 2, 6, 7, 8, 9},
		},
		{
			"many no overlap to right",
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			args{0, 4, 5},
			[]byte{0, 0, 0, 0, 5, 1, 2, 3, 4},
		},
		{
			"single no overlap to left",
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			args{8, 1, 2},
			[]byte{1, 2, 9, 4, 5, 6, 7, 8, 0},
		},
		{
			"double no overlap to left",
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			args{7, 2, 3},
			[]byte{1, 2, 3, 8, 9, 6, 7, 0, 0},
		},
		{
			"many no overlap to left",
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			args{5, 4, 0},
			[]byte{6, 7, 8, 9, 5, 0, 0, 0, 0},
		},
		{
			"double 1 overlap to right",
			[]byte{1, 1, 2, 2, 1, 1, 1, 1, 1, 1},
			args{2, 2, 3},
			[]byte{1, 1, 0, 2, 2, 1, 1, 1, 1, 1},
		},
		{
			"double 1 overlap to left",
			[]byte{1, 1, 1, 2, 2, 1, 1, 1, 1, 1},
			args{3, 2, 2},
			[]byte{1, 1, 2, 2, 0, 1, 1, 1, 1, 1},
		},
		{
			"triple 1 overlap to right",
			[]byte{1, 1, 2, 2, 2, 1, 1, 1, 1, 1},
			args{2, 3, 4},
			[]byte{1, 1, 0, 0, 2, 2, 2, 1, 1, 1},
		},
		{
			"triple 2 overlap to right",
			[]byte{1, 1, 2, 2, 2, 1, 1, 1, 1, 1},
			args{2, 3, 3},
			[]byte{1, 1, 0, 2, 2, 2, 1, 1, 1, 1},
		},
		{
			"triple 1 overlap to left",
			[]byte{1, 1, 1, 1, 2, 2, 2, 1, 1, 1},
			args{4, 3, 2},
			[]byte{1, 1, 2, 2, 2, 0, 0, 1, 1, 1},
		},
		{
			"triple 2 overlap to left",
			[]byte{1, 1, 1, 2, 2, 2, 1, 1, 1, 1},
			args{3, 3, 2},
			[]byte{1, 1, 2, 2, 2, 0, 1, 1, 1, 1},
		},
		{
			"no length",
			[]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			args{4, 0, 2},
			[]byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			p := &Page{
				data: tt.data,
			}
			p.moveAndZero(tt.args.offset, tt.args.size, tt.args.target)
			assert.Equal(tt.want, p.data)
		})
	}
}
