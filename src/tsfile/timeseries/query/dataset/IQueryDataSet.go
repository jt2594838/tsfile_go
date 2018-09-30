package dataset

import "tsfile/timeseries/read/datatype"

type IQueryDataSet interface {
	HasNext() bool

	Next() *datatype.RowRecord

	Close()
}
