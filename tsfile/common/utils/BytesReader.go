package utils

import (
	"encoding/binary"
	_ "log"
	"math"
)

// bytes slice reader, result is the reference of source
type BytesReader struct {
	buf []byte
	pos int
}

func NewBytesReader(data []byte) *BytesReader {
	return &BytesReader{data, 0}
}

func (r *BytesReader) Pos() int {
	return r.pos
}

func (r *BytesReader) Len() int {
	return len(r.buf) - r.pos
}

func (r *BytesReader) Remaining() []byte {
	return r.buf[r.pos:]
}

func (r *BytesReader) ReadBool() bool {
	result := (r.buf[r.pos] == 1)
	r.pos += 1

	return result
}

func (r *BytesReader) ReadShort() int16 {
	result := int16(binary.BigEndian.Uint16(r.buf[r.pos : r.pos+2]))
	r.pos += 2

	return result
}

func (r *BytesReader) ReadInt() int32 {
	bytes := r.buf[r.pos : r.pos+4]
	result := int32(binary.BigEndian.Uint32(bytes))
	r.pos += 4

	return result
}

func (r *BytesReader) ReadLong() int64 {
	result := int64(binary.BigEndian.Uint64(r.buf[r.pos : r.pos+8]))
	r.pos += 8

	return result
}

func (r *BytesReader) ReadFloat() float32 {
	bits := binary.BigEndian.Uint32(r.buf[r.pos : r.pos+4])
	result := math.Float32frombits(bits)
	r.pos += 4

	return result
}

func (r *BytesReader) ReadDouble() float64 {
	bits := binary.BigEndian.Uint64(r.buf[r.pos : r.pos+8])
	result := math.Float64frombits(bits)
	r.pos += 8

	return result
}

func (r *BytesReader) ReadString() string {
	length := int(r.ReadInt())
	result := string(r.buf[r.pos : r.pos+length])
	r.pos += length

	return result
}

func (r *BytesReader) ReadBytes(length int) []byte {
	dst := make([]byte, length)
	copy(dst, r.buf[r.pos:r.pos+length])

	r.pos += length

	return dst
}

func (r *BytesReader) ReadStringBinary() []byte {
	length := int(r.ReadInt())

	dst := make([]byte, length)
	copy(dst, r.buf[r.pos:r.pos+length])

	r.pos += length

	return dst
}

func (r *BytesReader) ReadSlice(length int) []byte {
	result := r.buf[r.pos : r.pos+length]
	r.pos += length

	return result
}

// read a byte
func (r *BytesReader) Read() int32 {
	result := r.buf[r.pos]
	r.pos++

	return int32(result)
}

// for decoding
func (r *BytesReader) ReadUnsignedVarInt() int32 {
	var value int32 = 0
	var i uint32 = 0

	b := r.buf[r.pos]
	r.pos++

	for r.pos <= len(r.buf) && (b&0x80) != 0 {
		value |= int32(b&0x7F) << i
		i += 7

		b = r.buf[r.pos]
		r.pos++
	}

	return value | int32(b<<i)
}
