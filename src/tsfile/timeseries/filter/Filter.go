package filter

type Filter interface {
	Satisfy(val interface{}) bool
}