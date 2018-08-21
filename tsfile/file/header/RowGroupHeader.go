package header

import (
	//	"bufio"
	//"log"
	"os"
	"tsfile/common/utils"
)

type RowGroupHeader struct {
	DeltaObjectID  string
	DataSize       int64
	NumberOfChunks int
	SerializedSize int
}

func (f *RowGroupHeader) DeserializeFrom(reader *os.File) {
	f.DeltaObjectID = utils.ReadString(reader)
	f.DataSize = utils.ReadLong(reader)
	f.NumberOfChunks = utils.ReadInt(reader)
	f.SerializedSize = utils.INT_LEN + len(f.DeltaObjectID) + utils.LONG_LEN + utils.INT_LEN
}
