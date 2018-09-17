package statistics

import (
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Integer struct {
	max   int32
	min   int32
	first int32
	last  int32
	sum   float64
}

func (s *Integer) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadInt()
	s.max = reader.ReadInt()
	s.first = reader.ReadInt()
	s.last = reader.ReadInt()
	s.sum = reader.ReadDouble()
}

func (s *Integer) GetSerializedSize() int {
	return 4*constant.INT_LEN + constant.DOUBLE_LEN
}
