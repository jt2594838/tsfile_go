package utils

import (
	//"bufio"
	"encoding/binary"

	"log"
	"math"
	"os"
)

const (
	BOOLEAN_LEN int = 1
	SHORT_LEN   int = 2
	INT_LEN     int = 4
	LONG_LEN    int = 8
	FLOAT_LEN   int = 4
	DOUBLE_LEN  int = 8
)

func ReadBool(reader *os.File) bool {
	buf := make([]byte, BOOLEAN_LEN)
	reader.Read(buf)
	result := BytsfileoBool(buf)

	return result
}

func ReadFloat(reader *os.File) float32 {
	buf := make([]byte, FLOAT_LEN)
	reader.Read(buf)
	bits := binary.LittleEndian.Uint32(buf)
	result := math.Float32frombits(bits)

	return result
}

func ReadDouble(reader *os.File) float64 {
	buf := make([]byte, DOUBLE_LEN)
	reader.Read(buf)
	bits := binary.LittleEndian.Uint64(buf)
	result := math.Float64frombits(bits)

	return result
}

func ReadShort(reader *os.File) int16 {
	buf := make([]byte, SHORT_LEN)
	reader.Read(buf)
	//result := binary.LittleEndian.Uint16(buf)
	result := BytsfileoShort(buf)

	return int16(result)
}

func ReadInt(reader *os.File) int {
	buf := make([]byte, INT_LEN)
	reader.Read(buf)
	//result := binary.LittleEndian.Uint32(buf)
	result := BytsfileoInt(buf)

	return int(result)
}

func ReadLong(reader *os.File) int64 {
	buf := make([]byte, LONG_LEN)
	reader.Read(buf)

	//result := binary.LittleEndian.Uint64(buf)
	result := BytsfileoLong(buf)

	return result
}

func ReadString(reader *os.File) string {
	len_bytes := make([]byte, INT_LEN)
	reader.Read(len_bytes)
	len := binary.BigEndian.Uint32(len_bytes)
	log.Println(len)

	data := make([]byte, len)
	reader.Read(data)
	result := string(data)
	log.Println(result)

	return result
}
