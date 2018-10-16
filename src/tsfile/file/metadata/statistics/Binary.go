package statistics

import (
	"tsfile/common/utils"
)

type Binary struct {
	//max   string
	//min   string
	//first string
	//last  string
	//sum   float64 //meaningless
	max     []byte
	min     []byte
	first   []byte
	last    []byte
	sum     float64 //meaningless
	isEmpty bool
}

func (s *Binary) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadStringBinary()
	s.max = reader.ReadStringBinary()
	s.first = reader.ReadStringBinary()
	s.last = reader.ReadStringBinary()
	s.sum = reader.ReadDouble()
}

func (b *Binary) SizeOfDaum() int {
	return -1
}

func (b *Binary) GetMaxByte(tdt int16) []byte {
	return []byte(b.max)
}

func (b *Binary) GetMinByte(tdt int16) []byte {
	return []byte(b.min)
}

func (b *Binary) GetFirstByte(tdt int16) []byte {
	return []byte(b.first)
}

func (b *Binary) GetLastByte(tdt int16) []byte {
	return []byte(b.last)
}

func (b *Binary) GetSumByte(tdt int16) []byte {
	return utils.Float64ToByte(b.sum, 0)
}

func (b *Binary) UpdateStats(fValue interface{}) {
	value := fValue.([]byte)
	if b.isEmpty {
		b.InitializeStats(value, value, value, value, 0)
		b.isEmpty = true
	} else {
		b.UpdateValue(value, value, value, value, 0)
	}
}

func (b *Binary) UpdateValue(max []byte, min []byte, first []byte, last []byte, sum float64) {
	// todo compare two []byte
	//if max > b.max {
	//	b.max = max
	//}
	//if min < b.min {
	//	b.min = min
	//}
	b.last = last
}

func (b *Binary) InitializeStats(max []byte, min []byte, first []byte, last []byte, sum float64) {
	b.max = max
	b.min = min
	b.first = first
	b.last = last
	b.sum = sum
}

func (s *Binary) GetSerializedSize() int {
	return 4*4 + len(s.max) + len(s.min) + len(s.first) + len(s.last)
}
