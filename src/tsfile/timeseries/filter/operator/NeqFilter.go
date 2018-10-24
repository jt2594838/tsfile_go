package operator

import "strings"

// NeqFilters compare the input value to the Reference value, and return true iff the input != the Reference.
// Type mismatch will set the return value to false. Use lexicographical order for strings.
// Supported types: int32(int) int64(long) float32(float) float64(double) string.
type IntNeqFilter struct {
	Ref int32
}

func (f *IntNeqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return f.Ref != v
	}
	return false
}

type LongNeqFilter struct {
	Ref int64
}

func (f *LongNeqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return f.Ref != v
	}
	return false
}

type StrNeqFilter struct {
	Ref string
}

func (f *StrNeqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.Ref) != 0
	}
	return false
}

type FloatNeqFilter struct {
	Ref float32
}

func (f *FloatNeqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v != f.Ref
	}
	return false
}

type DoubleNeqFilter struct {
	Ref float64
}

func (f *DoubleNeqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v != f.Ref
	}
	return false
}
