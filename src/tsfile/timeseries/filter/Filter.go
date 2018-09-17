package filter

type Filter interface {
	satisfy(val interface{}) bool
}