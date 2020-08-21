package page

// ID is the type of a page ID. This is mainly to avoid any confusion. Changing
// this will break existing database files, so only change during major version
// upgrades.
type ID = uint32

// DecodeID decodes a page ID from the given bytes. The ID is stored in the
// first 4 bytes, encoded big endian unsigned.
func DecodeID(data []byte) ID {
	return byteOrder.Uint32(data)
}
