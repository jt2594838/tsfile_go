package statistics

import (
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Integer struct {
	max   int
	min   int
	first int
	last  int
	sum   float64
}

func (s *Integer) DeserializeFrom(reader *utils.FileReader) {
	s.min = reader.ReadInt()
	s.max = reader.ReadInt()
	s.first = reader.ReadInt()
	s.last = reader.ReadInt()
	s.sum = reader.ReadDouble()
}

func (s *Integer) GetSerializedSize() int {
	return 4*constant.INT_LEN + constant.DOUBLE_LEN
}
