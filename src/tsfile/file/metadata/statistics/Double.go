package statistics

import (
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Double struct {
	max   float64
	min   float64
	first float64
	last  float64
	sum   float64
}

func (s *Double) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadDouble()
	s.max = reader.ReadDouble()
	s.first = reader.ReadDouble()
	s.last = reader.ReadDouble()
	s.sum = reader.ReadDouble()
}

func (s *Double) GetSerializedSize() int {
	return constant.DOUBLE_LEN * 5
}
