package operator

import "strings"

type IntGtFilter struct {
	ref int32
}

func (f *IntGtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return v > f.ref
	}
	return false
}

type LongGtFilter struct {
	ref int64
}

func (f *LongGtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return v > f.ref
	}
	return false
}

type StrGtFilter struct {
	ref string
}

func (f *StrGtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.ref) > 0
	}
	return false
}

type FloatGtFilter struct {
	ref float32
}

func (f *FloatGtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v > f.ref
	}
	return false
}

type DoubleGtFilter struct {
	ref float64
}

func (f *DoubleGtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v > f.ref
	}
	return false
}

