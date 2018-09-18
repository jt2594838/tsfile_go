package operator

import "strings"

type IntGtEqFilter struct {
	ref int32
}

func (f *IntGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return v >= f.ref
	}
	return false
}

type LongGtEqFilter struct {
	ref int64
}

func (f *LongGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return v >= f.ref
	}
	return false
}

type StrGtEqFilter struct {
	ref string
}

func (f *StrGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.ref) >= 0
	}
	return false
}

type FloatGtEqFilter struct {
	ref float32
}

func (f *FloatGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v >= f.ref
	}
	return false
}

type DoubleGtEqFilter struct {
	ref float64
}

func (f *DoubleGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v >= f.ref
	}
	return false
}

