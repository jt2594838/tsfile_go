package statistics

import (
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Long struct {
	max   int64
	min   int64
	first int64
	last  int64
	sum   float64
}

func (s *Long) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadLong()
	s.max = reader.ReadLong()
	s.first = reader.ReadLong()
	s.last = reader.ReadLong()
	s.sum = reader.ReadDouble()
}

func (s *Long) GetSerializedSize() int {
	return 4*constant.LONG_LEN + constant.DOUBLE_LEN
}
