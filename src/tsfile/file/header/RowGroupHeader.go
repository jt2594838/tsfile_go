package header

import (
	_ "bufio"
	"bytes"
	_ "log"
	_ "os"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type RowGroupHeader struct {
	device         string
	dataSize       int64
	numberOfChunks int32
	serializedSize int32
}

func (h *RowGroupHeader) Deserialize(reader *utils.FileReader) {
	h.device = reader.ReadString()
	h.dataSize = reader.ReadLong()
	h.numberOfChunks = reader.ReadInt()

	h.serializedSize = int32(constant.INT_LEN + len(h.device) + constant.LONG_LEN + constant.INT_LEN)
}

func (h *RowGroupHeader) GetDevice() string {
	return h.device
}

func (h *RowGroupHeader) GetDataSize() int64 {
	return h.dataSize
}

func (h *RowGroupHeader) GetNumberOfChunks() int32 {
	return h.numberOfChunks
}

func (h *RowGroupHeader) GetSerializedSize() int32 {
	return h.serializedSize
}

func (r *RowGroupHeader) RowGroupHeaderToMemory(buffer *bytes.Buffer) int32 {
	// write header to buffer
	buffer.Write(utils.Int32ToByte(int32(len(r.device)), 0))
	buffer.Write([]byte(r.device))
	buffer.Write(utils.Int64ToByte(r.dataSize, 0))
	buffer.Write(utils.Int32ToByte(r.numberOfChunks, 0))

	return r.serializedSize
}

func GetRowGroupSerializedSize(deviceId string) int {
	return 1*4 + 1*8 + len(deviceId) + 1*4
}

func NewRowGroupHeader(dId string, rgs int64, sn int32) (*RowGroupHeader, error) {
	ss := 1*4 + 1*8 + len(dId) + 1*4
	return &RowGroupHeader{
		device:         dId,
		dataSize:       rgs,
		numberOfChunks: sn,
		serializedSize: int32(ss),
	}, nil
}
