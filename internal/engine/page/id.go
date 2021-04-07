package page

import "unsafe"

// ID is the type of a page ID. This is mainly to avoid any confusion. Changing
// this will break existing database files, so only change during major version
// upgrades.
type ID = uint32

// IDSize is the byte size of the ID type.
const IDSize = unsafe.Sizeof(ID(0)) // #nosec

// DecodeID decodes a page ID from the given bytes. The ID is stored in the
// first 4 bytes (IDSize), encoded big endian unsigned.
func DecodeID(data []byte) ID {
	return byteOrder.Uint32(data)
}
