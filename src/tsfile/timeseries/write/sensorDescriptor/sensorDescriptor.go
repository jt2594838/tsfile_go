package sensorDescriptor

/**
 * @Package Name: measurementDescriptor
 * @Author: steven yao
 * @Email:  yhp.linux@gmail.com
 * @Create Date: 18-8-24 下午7:38
 * @Description:
 */

import (
	"tsfile/compress"
	"tsfile/common/constant"
	"tsfile/encoding/encoder"
)

type SensorDescriptor struct {
	sensorId			string
	tsDataType			int16
	tsEncoding			int16
	timeCount			int
	compressor			*compress.Encompress
	tsCompresstionType	int16

	//typeConverter		TsDataTypeConverter
	//encodingConverter	TsEncodingConverter

	//conf 				TsFileConfig
	//props 				make(map[string]string)
}

func (s *SensorDescriptor) GetTimeCount() (int) {
	return s.timeCount
}

func (s *SensorDescriptor) SetTimeCount(count int) () {
	s.timeCount = count
	return
}

func (s *SensorDescriptor) GetSensorId() (string) {
	return s.sensorId
}

func (s *SensorDescriptor) GetTsDataType() (int16) {
	return s.tsDataType
}

func (s *SensorDescriptor) GetTsEncoding() (int16) {
	return s.tsEncoding
}

func (s *SensorDescriptor) GetCompresstionType() (int16) {
	return s.tsCompresstionType
}

// the return type should be Compressor, after finished Compressor we should modify it.
func (s *SensorDescriptor) GetCompressor() (*compress.Encompress) {
	return s.compressor
}

func (s *SensorDescriptor) GetTimeEncoder() (encoder.Encoder) {
	return encoder.GetEncoder(int16(constant.TS_2DIFF), int16(constant.INT64))
}

func (s *SensorDescriptor) GetValueEncoder() (encoder.Encoder) {
	return encoder.GetEncoder(s.GetTsEncoding(), s.GetTsDataType())
}

func (s *SensorDescriptor) Close() (bool) {
	return true
}


func New(sId string, tdt constant.TSDataType, te constant.TSEncoding) (*SensorDescriptor, error) {
	// init compressor
	enCompressor := new(compress.Encompress)
	return &SensorDescriptor{
		sensorId:sId,
		tsDataType:int16(tdt),
		tsEncoding:int16(te),
		compressor:enCompressor,
		tsCompresstionType:int16(constant.UNCOMPRESSED),
		timeCount:-1,
		},nil
}