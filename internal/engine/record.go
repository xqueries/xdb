package engine

import (
	"bytes"
	"fmt"

	"github.com/xqueries/xdb/internal/engine/table"
	"github.com/xqueries/xdb/internal/engine/types"
)

func serializeRow(row table.Row) ([]byte, error) {
	var buf bytes.Buffer

	for _, value := range row.Values {
		t := value.Type()
		if serializer, ok := t.(types.Serializer); ok {
			serialized, err := serializer.Serialize(value)
			if err != nil {
				return nil, fmt.Errorf("serialize: %w", err)
			}
			_, _ = buf.Write(frame(serialized))
		} else {
			return nil, fmt.Errorf("type %v is not serializable", t)
		}
	}

	return buf.Bytes(), nil
}

func deserializeRow(cols []table.Col, data []byte) (table.Row, error) {
	var serializers []types.Serializer
	for _, col := range cols {
		if serializer, ok := col.Type.(types.Serializer); ok {
			serializers = append(serializers, serializer)
		} else {
			return table.Row{}, fmt.Errorf("type %v is not deserializable", col.Type)
		}
	}

	buf := bytes.NewBuffer(data)
	var vals []types.Value
	for i := range serializers {
		// read frame
		frame := make([]byte, 4)
		n, err := buf.Read(frame)
		if err != nil {
			return table.Row{}, fmt.Errorf("read frame: %w", err)
		}
		if n != 4 {
			return table.Row{}, fmt.Errorf("read frame: expected 4 bytes, could only read %v", n)
		}
		// read record
		recBuf := make([]byte, byteOrder.Uint32(frame))
		n, err = buf.Read(recBuf)
		if err != nil {
			return table.Row{}, fmt.Errorf("read record: %w", err)
		}
		if n != len(recBuf) {
			return table.Row{}, fmt.Errorf("read record: expected %v bytes, could only read %v", len(recBuf), n)
		}
		// deserialize record
		val, err := serializers[i].Deserialize(recBuf)
		if err != nil {
			return table.Row{}, fmt.Errorf("deserialize column %v: %w", i, err)
		}
		vals = append(vals, val)
	}
	return table.Row{Values: vals}, nil
}
