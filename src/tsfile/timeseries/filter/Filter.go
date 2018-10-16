package filter

// Filter determines whether the given value (primitive, RowRecord) satisfies a certain
// condition(arithmetic or their logic combination).
type Filter interface {
	Satisfy(val interface{}) bool
}
