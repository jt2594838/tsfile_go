package operator

import "strings"

// EqFilters compare the input value to the Reference value, and return true iff they are equal.
// Type mismatch will set the return value to false.
// Supported types: int32(int) int64(long) float32(float) float64(double) string.

type IntEqFilter struct {
	Ref int32
}

func (f *IntEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return f.Ref == v
	}
	return false
}

type LongEqFilter struct {
	Ref int64
}

func (f *LongEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return f.Ref == v
	}
	return false
}

type StrEqFilter struct {
	Ref string
}

func (f *StrEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.Ref) == 0
	}
	return false
}

type FloatEqFilter struct {
	Ref float32
}

func (f *FloatEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v == f.Ref
	}
	return false
}

type DoubleEqFilter struct {
	Ref float64
}

func (f *DoubleEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v == f.Ref
	}
	return false
}
