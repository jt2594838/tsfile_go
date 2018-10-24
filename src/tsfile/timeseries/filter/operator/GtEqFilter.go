package operator

import "strings"

// GtEqFilters compare the input value to the Reference value, and return true iff the input >= the Reference.
// Type mismatch will set the return value to false. Use lexicographical order for strings.
// Supported types: int32(int) int64(long) float32(float) float64(double) string.
type IntGtEqFilter struct {
	Ref int32
}

func (f *IntGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return v >= f.Ref
	}
	return false
}

type LongGtEqFilter struct {
	Ref int64
}

func (f *LongGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return v >= f.Ref
	}
	return false
}

type StrGtEqFilter struct {
	Ref string
}

func (f *StrGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.Ref) >= 0
	}
	return false
}

type FloatGtEqFilter struct {
	Ref float32
}

func (f *FloatGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v >= f.Ref
	}
	return false
}

type DoubleGtEqFilter struct {
	Ref float64
}

func (f *DoubleGtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v >= f.Ref
	}
	return false
}
