package utils

import (
	"io"
	//"bufio"
	"encoding/binary"
	_ "log"
	"math"
	"os"
	"tsfile/common/constant"
)

// TO DO: add buffer for performence purpose
type FileReader struct {
	reader *os.File
	pos    int64
}

func NewFileReader(reader *os.File) *FileReader {
	return &FileReader{reader, 0}
}

func (f *FileReader) Close() error {
	return f.reader.Close()
}

func (f *FileReader) Seek(pos int64, whence int) (ret int64, err error) {
	var e error
	f.pos, e = f.reader.Seek(pos, whence)

	return f.pos, e
}

func (f *FileReader) Pos() int64 {
	return f.pos
}

func (f *FileReader) Skip(length int) (ret int64, err error) {
	return f.Seek(int64(length), io.SeekCurrent)
}

func (f *FileReader) ReadBool() bool {
	buf := make([]byte, constant.BOOLEAN_LEN)
	n, err := f.reader.Read(buf)
	if err != nil && err != io.EOF && n != constant.BOOLEAN_LEN {
		panic(err)
	}

	result := (buf[0] == 1)
	f.pos += int64(constant.BOOLEAN_LEN)

	return result
}

func (f *FileReader) ReadShort() int16 {
	buf := make([]byte, constant.SHORT_LEN)
	n, err := f.reader.Read(buf)
	if err != nil && err != io.EOF && n != constant.SHORT_LEN {
		panic(err)
	}

	result := int16(binary.BigEndian.Uint16(buf))
	f.pos += int64(constant.SHORT_LEN)

	return result
}

func (f *FileReader) ReadInt() int {
	buf := make([]byte, constant.INT_LEN)
	n, err := f.reader.Read(buf)
	if err != nil && err != io.EOF && n != constant.INT_LEN {
		panic(err)
	}

	result := int(binary.BigEndian.Uint32(buf))
	f.pos += int64(constant.INT_LEN)

	return result
}

func (f *FileReader) ReadLong() int64 {
	buf := make([]byte, constant.LONG_LEN)
	n, err := f.reader.Read(buf)
	if err != nil && err != io.EOF && n != constant.LONG_LEN {
		panic(err)
	}

	result := int64(binary.BigEndian.Uint64(buf))
	f.pos += int64(constant.LONG_LEN)

	return result
}

func (f *FileReader) ReadFloat() float32 {
	buf := make([]byte, constant.FLOAT_LEN)
	n, err := f.reader.Read(buf)
	if err != nil && err != io.EOF && n != constant.FLOAT_LEN {
		panic(err)
	}

	bits := binary.BigEndian.Uint32(buf)
	result := math.Float32frombits(bits)

	f.pos += int64(constant.FLOAT_LEN)

	return result
}

func (f *FileReader) ReadDouble() float64 {
	buf := make([]byte, constant.DOUBLE_LEN)
	n, err := f.reader.Read(buf)
	if err != nil && err != io.EOF && n != constant.DOUBLE_LEN {
		panic(err)
	}

	bits := binary.BigEndian.Uint64(buf)
	result := math.Float64frombits(bits)
	f.pos += int64(constant.DOUBLE_LEN)

	return result
}

func (f *FileReader) ReadString() string {
	length := f.ReadInt()

	buf := make([]byte, length)
	n, err := f.reader.Read(buf)
	if err != nil && err != io.EOF && n != length {
		panic(err)
	}

	result := string(buf)
	f.pos += int64(constant.INT_LEN) + int64(length)

	return result
}

func (f *FileReader) ReadSlice(length int) []byte {
	len_bytes := make([]byte, length)
	n, err := f.reader.Read(len_bytes)
	if err != nil && err != io.EOF && n != length {
		panic(err)
	}

	f.pos += int64(length)

	return len_bytes
}

func (f *FileReader) ReadAt(length int, pos int64) []byte {
	len_bytes := make([]byte, length)
	n, err := f.reader.ReadAt(len_bytes, pos)
	if err != nil && err != io.EOF && n != length {
		panic(err)
	}
	//f.pos += int64(length)

	return len_bytes
}
