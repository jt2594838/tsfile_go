package header

import (
	_ "bufio"
	_ "log"
	_ "os"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type RowGroupHeader struct {
	device         string
	dataSize       int64
	numberOfChunks int
	serializedSize int
}

func (h *RowGroupHeader) Deserialize(reader *utils.FileReader) {
	h.device = reader.ReadString()
	h.dataSize = reader.ReadLong()
	h.numberOfChunks = int(reader.ReadInt())

	h.serializedSize = constant.INT_LEN + len(h.device) + constant.LONG_LEN + constant.INT_LEN
}

func (h *RowGroupHeader) GetDevice() string {
	return h.device
}

func (h *RowGroupHeader) GetDataSize() int64 {
	return h.dataSize
}

func (h *RowGroupHeader) GetNumberOfChunks() int {
	return h.numberOfChunks
}

func (h *RowGroupHeader) GetSerializedSize() int {
	return h.serializedSize
}
