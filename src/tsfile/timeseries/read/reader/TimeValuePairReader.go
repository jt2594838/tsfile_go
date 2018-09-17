package reader

import (
	"tsfile/timeseries/read/datatype"
)

type TimeValuePairReader interface {
	Read(data []byte)

	HasNext() bool

	Next() datatype.TimeValuePair

	Skip()

	Close()
}
