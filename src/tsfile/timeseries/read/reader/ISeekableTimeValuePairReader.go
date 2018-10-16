package reader

import "tsfile/timeseries/read/datatype"

type ISeekableTimeValuePairReader interface {
	TimeValuePairReader
	Seek(timestamp int64) bool
	Current() *datatype.TimeValuePair
}
