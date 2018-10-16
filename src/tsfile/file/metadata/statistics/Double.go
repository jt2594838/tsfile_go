package statistics

import (
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Double struct {
	max     float64
	min     float64
	first   float64
	last    float64
	sum     float64
	isEmpty bool
}

func (s *Double) Deserialize(reader *utils.FileReader) {
	s.min = reader.ReadDouble()
	s.max = reader.ReadDouble()
	s.first = reader.ReadDouble()
	s.last = reader.ReadDouble()
	s.sum = reader.ReadDouble()
}

func (d *Double) SizeOfDaum() int {
	return 4
}

func (d *Double) GetMaxByte(tdt int16) []byte {
	return utils.Float64ToByte(d.max, 0)
}

func (d *Double) GetMinByte(tdt int16) []byte {
	return utils.Float64ToByte(d.min, 0)
}

func (d *Double) GetFirstByte(tdt int16) []byte {
	return utils.Float64ToByte(d.first, 0)
}

func (d *Double) GetLastByte(tdt int16) []byte {
	return utils.Float64ToByte(d.last, 0)
}

func (d *Double) GetSumByte(tdt int16) []byte {
	return utils.Float64ToByte(d.sum, 0)
}

func (d *Double) UpdateStats(dValue interface{}) {
	value := dValue.(float64)
	if d.isEmpty {
		d.InitializeStats(value, value, value, value, value)
		d.isEmpty = true
	} else {
		d.UpdateValue(value, value, value, value, value)
	}
}

func (d *Double) UpdateValue(max float64, min float64, first float64, last float64, sum float64) {
	if max > d.max {
		d.max = max
	}
	if min < d.min {
		d.min = min
	}
	d.sum += sum
	d.last = last
}

func (d *Double) InitializeStats(max float64, min float64, first float64, last float64, sum float64) {
	d.max = max
	d.min = min
	d.first = first
	d.last = last
	d.sum = sum
}

func (s *Double) GetSerializedSize() int {
	return constant.DOUBLE_LEN * 5
}
