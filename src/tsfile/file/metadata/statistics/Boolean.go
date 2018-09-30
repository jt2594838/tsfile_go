package statistics

import (
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Boolean struct {
	max   bool
	min   bool
	first bool
	last  bool
	sum   float64
}

func (s *Boolean) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadBool()
	s.max = reader.ReadBool()
	s.first = reader.ReadBool()
	s.last = reader.ReadBool()
	s.sum = reader.ReadDouble()
}

func (s *Boolean) GetSerializedSize() int {
	return 4*constant.BOOLEAN_LEN + constant.DOUBLE_LEN
}
