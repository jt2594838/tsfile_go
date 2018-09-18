package operator

import "strings"

type IntEqFilter struct {
	ref int32
}

func (f *IntEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return f.ref == v
	}
	return false
}

type LongEqFilter struct {
	ref int64
}

func (f *LongEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return f.ref == v
	}
	return false
}

type StrEqFilter struct {
	ref string
}

func (f *StrEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.ref) == 0
	}
	return false
}

type FloatEqFilter struct {
	ref float32
}

func (f *FloatEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v == f.ref
	}
	return false
}

type DoubleEqFilter struct {
	ref float64
}

func (f *DoubleEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v == f.ref
	}
	return false
}

