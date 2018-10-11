package statistics

import (
	"strconv"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"bytes"
)

type Statistics interface {
	Deserialize(reader *utils.FileReader)
	GetSerializedSize() int
	GetMaxByte (tdt int16) ([]byte)
	GetMinByte (tdt int16) ([]byte)
	GetFirstByte (tdt int16) ([]byte)
	GetLastByte (tdt int16) ([]byte)
	GetSumByte (tdt int16) ([]byte)
	SizeOfDaum () (int)
	UpdateStats (value interface{}) ()
}

func Deserialize(reader *utils.FileReader, dataType constant.TSDataType) Statistics {
	var statistics Statistics

	switch dataType {
	case constant.BOOLEAN:
		statistics = new(Boolean)
	case constant.INT32:
		statistics = new(Integer)
	case constant.INT64:
		statistics = new(Long)
	case constant.FLOAT:
		statistics = new(Float)
	case constant.DOUBLE:
		statistics = new(Double)
	case constant.TEXT:
		statistics = new(Binary)
	default:
		panic("Statistics unknown dataType: " + strconv.Itoa(int(dataType)))
	}

	statistics.Deserialize(reader)

	return statistics
}

func GetStatsByType(tsDataType int16) (Statistics) {
	var statistics Statistics
	switch constant.TSDataType(tsDataType) {
	case constant.BOOLEAN:
		statistics = new(Boolean)
	case constant.INT32:
		statistics = new(Integer)
	case constant.INT64:
		statistics = new(Long)
	case constant.FLOAT:
		statistics = new(Float)
	case constant.DOUBLE:
		statistics = new(Double)
	case constant.TEXT:
		statistics = new(Binary)
	default:
		panic("Statistics unknown dataType: " + strconv.Itoa(int(tsDataType)))
	}
	return statistics
}

func Serialize (s Statistics, buffer *bytes.Buffer, tsDataType int16) (int) {
	var length int
	if s.SizeOfDaum() == 0 {
		return 0
	} else if s.SizeOfDaum() != -1 {
		buffer.Write(s.GetMaxByte(tsDataType))
		buffer.Write(s.GetMinByte(tsDataType))
		buffer.Write(s.GetFirstByte(tsDataType))
		buffer.Write(s.GetLastByte(tsDataType))
		buffer.Write(s.GetSumByte(tsDataType))
		length = s.SizeOfDaum() * 4 + 8
	} else {
		maxData := s.GetMaxByte(tsDataType)
		buffer.Write(utils.Int32ToByte(int32(len(maxData))))
		maxLen, _ :=buffer.Write(maxData)
		length += maxLen
		minData := s.GetMinByte(tsDataType)
		buffer.Write(utils.Int32ToByte(int32(len(minData))))
		minLen, _ := buffer.Write(minData)
		length += minLen
		firstData := s.GetMinByte(tsDataType)
		buffer.Write(utils.Int32ToByte(int32(len(firstData))))
		firstLen, _ := buffer.Write(firstData)
		length += firstLen
		lastData := s.GetLastByte(tsDataType)
		buffer.Write(utils.Int32ToByte(int32(len(lastData))))
		lastLen, _ := buffer.Write(lastData)
		length += lastLen
		sumData := s.GetSumByte(tsDataType)
		buffer.Write(utils.Int32ToByte(int32(len(sumData))))
		sumLen, _ := buffer.Write(sumData)
		length += sumLen
		length = length + 4 * 5
	}
	return length
}
