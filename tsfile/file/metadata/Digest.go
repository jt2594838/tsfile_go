package metadata

import (
	_ "log"
	"tsfile/common/constant"
	"tsfile/common/utils"
)

type Digest struct {
	statistics     map[string][]byte
	serializedSize int
}

func (f *Digest) DeserializeFrom(reader *utils.BytesReader) {
	f.serializedSize = constant.INT_LEN

	size := reader.ReadInt()
	f.statistics = make(map[string][]byte)
	if size > 0 {

		for i := 0; i < size; i++ {
			key := reader.ReadString()
			value := reader.ReadStringBinary()

			f.statistics[key] = value
			f.serializedSize += constant.INT_LEN + len(key) + constant.INT_LEN + len(value)
		}
	}
}

func (f *Digest) GetSerializedSize() int {
	return f.serializedSize
}
