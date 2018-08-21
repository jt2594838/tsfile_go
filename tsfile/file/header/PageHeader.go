package header

import (
	//	"bufio"
	//"log"
	"os"
	"tsfile/common/utils"
)

type PageHeader struct {
	UncompressedSize int
	CompressedSize   int
	NumberOfValues   int
	//Statistics       Statistics
	Max_timestamp  int64
	Min_timestamp  int64
	SerializedSize int
}

func (f *PageHeader) DeserializeFrom(reader *os.File) {
	f.UncompressedSize = utils.ReadInt(reader)
	f.CompressedSize = utils.ReadInt(reader)
	f.NumberOfValues = utils.ReadInt(reader)
	//f.Statistics = utils.ReadInt(reader)
	f.Max_timestamp = utils.ReadLong(reader)
	f.Min_timestamp = utils.ReadLong(reader)

	f.SerializedSize = 3*utils.INT_LEN + 2*utils.LONG_LEN // + statistics.getSerializedSize()
}
