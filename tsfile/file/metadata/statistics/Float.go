package statistics

import (
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Float struct {
	max   float32
	min   float32
	first float32
	last  float32
	sum   float64
}

func (s *Float) DeserializeFrom(reader *utils.FileReader) {
	s.min = reader.ReadFloat()
	s.max = reader.ReadFloat()
	s.first = reader.ReadFloat()
	s.last = reader.ReadFloat()
	s.sum = reader.ReadDouble()
}

func (s *Float) GetSerializedSize() int {
	return 4*constant.FLOAT_LEN + constant.DOUBLE_LEN
}
