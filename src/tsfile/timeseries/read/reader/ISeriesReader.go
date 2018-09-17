package reader

import "tsfile/timeseries/read/datatype"

type ISeriesReader interface {

	HasNext() bool

	Next() datatype.TimeValuePair

	Close()
}
