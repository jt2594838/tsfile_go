package reader

import "tsfile/timeseries/read/datatype"

type IRowRecordReader interface {
	HasNext() bool

	Next() *datatype.RowRecord

	Close()
}
