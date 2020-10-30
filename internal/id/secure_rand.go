package id

import (
	"crypto/rand"
	"encoding/binary"
	unsafeRand "math/rand"
	"unsafe"
)

var _ unsafeRand.Source = (*secureSource)(nil)

type secureSource struct {
}

// Uint64 creates a new, cryptographically secure random uint64.
func (s secureSource) Uint64() uint64 {
	buf := make([]byte, unsafe.Sizeof(int64(0))) // #nosec
	_, _ = rand.Read(buf)
	return binary.BigEndian.Uint64(buf)
}

// Int63 creates a new, cryptographically secure random int64.
func (s secureSource) Int63() int64 {
	return int64(s.Uint64())
}

// Seed is a no-op.
func (s secureSource) Seed(_ int64) {
	// no-op
}
