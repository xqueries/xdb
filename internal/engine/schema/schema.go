package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

var byteOrder = binary.BigEndian

// Schema is the schema of a single table.
type Schema struct {
	cols []table.Col
}

// New creates a new Schema from the given columns.
func New(cols []table.Col) *Schema {
	return &Schema{
		cols: cols,
	}
}

// Cols returns the columns in this table schema.
func (s *Schema) Cols() []table.Col {
	return s.cols
}

// WriteTo will serialize this schema onto the given writer.
// If a schema has been written with this method, it can be read
// with ReadFrom.
func (s *Schema) WriteTo(w io.Writer) (n int64, err error) {
	var buf bytes.Buffer

	for _, col := range s.cols {
		typeIndicator := types.IndicatorFor(col.Type)
		if typeIndicator == types.TypeIndicatorUnknown {
			return 0, fmt.Errorf("unknown type indicator for type %v", col.Type)
		}
		_ = buf.WriteByte(byte(typeIndicator))
		_, _ = buf.Write(frame([]byte(col.QualifiedName)))
	}

	return buf.WriteTo(w)
}

// ReadFrom will deserialize into this schema from the given reader.
// Use this for data that has been written with WriteTo.
func (s *Schema) ReadFrom(r io.Reader) (n int64, err error) {
	var colInfo bytes.Buffer
	totalRead, err := colInfo.ReadFrom(r)
	if err != nil {
		return totalRead, err
	}

	for {
		// type indicator
		typeIndicator, err := colInfo.ReadByte()
		if err != nil && err != io.EOF {
			return totalRead, fmt.Errorf("read type indicator: %w", err)
		} else if err == io.EOF {
			break
		}
		// col name frame
		frame := make([]byte, 4)
		n, err := colInfo.Read(frame)
		if err != nil {
			return totalRead, fmt.Errorf("read frame: %w", err)
		}
		if n != 4 {
			return totalRead, fmt.Errorf("read frame: expected %v bytes, could only read %v", 4, n)
		}
		// col name
		buf := make([]byte, byteOrder.Uint32(frame))
		n, err = colInfo.Read(buf)
		if err != nil {
			return totalRead, fmt.Errorf("read col name: %w", err)
		}
		if n != len(buf) {
			return totalRead, fmt.Errorf("read col name: expected %v bytes, could only read %v", len(buf), n)
		}
		// col read successfully, append to underlyingColumns
		s.cols = append(s.cols, table.Col{
			QualifiedName: string(buf),
			Type:          types.ByIndicator(types.TypeIndicator(typeIndicator)),
		})
	}
	return totalRead, nil
}

func frame(data []byte) []byte {
	buf := make([]byte, 4+len(data))
	byteOrder.PutUint32(buf, uint32(len(data)))
	copy(buf[4:], data)
	return buf
}
