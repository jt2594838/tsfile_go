package reader

import (
	"tsfile/timeseries/read/datatype"
)

type TimeValuePairReader interface {
	HasNext() bool

	next() datatype.TimeValuePair

	SkipCurrentTimeValuePair()

	Close()
}
