package dataset

import "tsfile/timeseries/read/datatype"

// IQueryDataSet is an iterator of RowRecords which satisfy a QueryExpression that is given to and executed by QueryEngine.
// You may call HasNext() to check whether there is still more unread values before calling Next(), or you can check
// whether the error returned by Next() is null.
type IQueryDataSet interface {
	HasNext() bool

	Next() (*datatype.RowRecord, error)

	Close()
}
