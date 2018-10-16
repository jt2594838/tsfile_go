package query

import "tsfile/timeseries/filter"

type QueryExpression struct {
	selectPaths    []string
	conditionPaths []string
	filter         filter.Filter
}

func (q *QueryExpression) ConditionPaths() []string {
	return q.conditionPaths
}

func (q *QueryExpression) SetConditionPaths(conditionPaths []string) {
	q.conditionPaths = conditionPaths
}

func (q *QueryExpression) SetFilter(filter filter.Filter) {
	q.filter = filter
}

func (q *QueryExpression) SetSelectPaths(paths []string) {
	q.selectPaths = paths
}

func (q *QueryExpression) Filter() filter.Filter {
	return q.filter
}

func (q *QueryExpression) SelectPaths() []string {
	return q.selectPaths
}
