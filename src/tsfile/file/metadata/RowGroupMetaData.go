package metadata

import (
	//_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
	"tsfile/common/log"
	"bytes"
)

type RowGroupMetaData struct {
	device                        string
	totalByteSize                 int64
	fileOffsetOfCorrespondingData int64
	serializedSize                int
	ChunkMetaDataSli   []*ChunkMetaData
	sizeOfChunkSli                int
}

func (f *RowGroupMetaData) Deserialize(reader *utils.BytesReader) {
	f.device = reader.ReadString()
	f.totalByteSize = reader.ReadLong()
	f.fileOffsetOfCorrespondingData = reader.ReadLong()
	size := int(reader.ReadInt())

	f.serializedSize = constant.INT_LEN + len(f.device) + constant.LONG_LEN + constant.INT_LEN

	f.ChunkMetaDataSli = make([]*ChunkMetaData, 0)
	for i := 0; i < size; i++ {
		chunkMetaData := new(ChunkMetaData)
		chunkMetaData.Deserialize(reader)
		f.ChunkMetaDataSli = append(f.ChunkMetaDataSli, chunkMetaData)
		f.serializedSize += chunkMetaData.GetSerializedSize()
	}
}

func (f *RowGroupMetaData) GetSerializedSize() int {
	return f.serializedSize
}

func (r *RowGroupMetaData) AddChunkMetaData (md *ChunkMetaData) () {
	if len(r.ChunkMetaDataSli) == 0 {
		r.ChunkMetaDataSli = make([]*ChunkMetaData, 0)
	}
	r.ChunkMetaDataSli = append(r.ChunkMetaDataSli, md)
	r.serializedSize += md.GetSerializedSize()
	r.sizeOfChunkSli += 1
}

func (r *RowGroupMetaData) SetTotalByteSize (ms int64) () {
	r.totalByteSize = ms
}

func (r *RowGroupMetaData) GetDeviceId () (string) {
	return r.device
}

func (r *RowGroupMetaData) SerializeTo (buf *bytes.Buffer) (int) {
	if r.sizeOfChunkSli != len(r.ChunkMetaDataSli) {
		r.RecalculateSerializedSize()
	}
	var byteLen int

	n1, _ := buf.Write(utils.Int32ToByte(int32(len(r.device))))
	byteLen += n1
	n2, _ := buf.Write([]byte(r.device))
	byteLen += n2

	n3, _ := buf.Write(utils.Int64ToByte(r.totalByteSize))
	byteLen += n3
	n4, _ := buf.Write(utils.Int64ToByte(r.fileOffsetOfCorrespondingData))
	byteLen += n4

	n5, _ := buf.Write(utils.Int32ToByte(int32(len(r.ChunkMetaDataSli))))
	byteLen += n5
	for _, v := range r.ChunkMetaDataSli {
		byteLen += v.SerializeTo(buf)
	}

	return byteLen
}

func (r *RowGroupMetaData) GetChunkMetaDataSli () ([]*ChunkMetaData) {
	//if r.ChunkMetaDataSli == nil {
	//	return nil
	//}
	return r.ChunkMetaDataSli
}

func (r *RowGroupMetaData) GetserializedSize () (int) {
	if r.sizeOfChunkSli != len(r.ChunkMetaDataSli) {
		r.RecalculateSerializedSize()
	}
	return r.serializedSize
}

func (r *RowGroupMetaData) RecalculateSerializedSize () () {
	r.serializedSize = 1 *4 + len(r.device) + 2 * 8 + 1 * 4
	for _, v := range r.ChunkMetaDataSli {
		if &v != nil {
			r.serializedSize += v.GetSerializedSize()
			log.Info("ChunkMetaDataSliaaaaaa: %s", v)
		}
	}
	r.sizeOfChunkSli = len(r.ChunkMetaDataSli)
	return
}

func NewRowGroupMetaData(dId string, tbs int64, foocd int64, tscmds []*ChunkMetaData) (*RowGroupMetaData, error) {
	return &RowGroupMetaData{
		device:dId,
		totalByteSize:tbs,
		fileOffsetOfCorrespondingData:foocd,
		ChunkMetaDataSli:tscmds,
	},nil
}
