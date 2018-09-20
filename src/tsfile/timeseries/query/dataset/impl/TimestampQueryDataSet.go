package impl

import "tsfile/timeseries/read/datatype"

type TimestampQueryDataSet struct {

}

func (TimestampQueryDataSet) HasNext() bool {
	panic("implement me")
}

func (TimestampQueryDataSet) Next() *datatype.RowRecord {
	panic("implement me")
}

func (TimestampQueryDataSet) Close() {
	panic("implement me")
}

