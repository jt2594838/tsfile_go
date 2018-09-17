package query

import "tsfile/timeseries/filter"

type QueryExpression struct {
	paths []string
	filter filter.Filter
}

func (q *QueryExpression) Filter() filter.Filter {
	return q.filter
}

func (q *QueryExpression) Paths() []string {
	return q.paths
}

