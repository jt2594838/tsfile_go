package reader

import "tsfile/timeseries/read/datatype"

type ISeekableRowReader interface {
	IRowRecordReader

	Current() *datatype.RowRecord

	Seek(timestamp int64) bool
}
