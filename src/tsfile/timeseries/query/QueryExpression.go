package query

import "tsfile/timeseries/filter"

type QueryExpression struct {
	paths []string
	filter filter.Filter
}
