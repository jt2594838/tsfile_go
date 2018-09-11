package statistics

import (
	"tsfile/common/utils"
)

type Binary struct {
	max   string
	min   string
	first string
	last  string
	sum   float64 //meaningless
}

func (s *Binary) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadString()
	s.max = reader.ReadString()
	s.first = reader.ReadString()
	s.last = reader.ReadString()
	s.sum = reader.ReadDouble()
}

func (s *Binary) GetSerializedSize() int {
	return 4*4 + len(s.max) + len(s.min) + len(s.first) + len(s.last)
}
