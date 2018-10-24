package operator

import "strings"

// GtFilters compare the input value to the Reference value, and return true iff the input > the Reference.
// Type mismatch will set the return value to false. Use lexicographical order for strings.
// Supported types: int32(int) int64(long) float32(float) float64(double) string.
type IntGtFilter struct {
	Ref int32
}

func (f *IntGtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return v > f.Ref
	}
	return false
}

type LongGtFilter struct {
	Ref int64
}

func (f *LongGtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return v > f.Ref
	}
	return false
}

type StrGtFilter struct {
	Ref string
}

func (f *StrGtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.Ref) > 0
	}
	return false
}

type FloatGtFilter struct {
	Ref float32
}

func (f *FloatGtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v > f.Ref
	}
	return false
}

type DoubleGtFilter struct {
	Ref float64
}

func (f *DoubleGtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v > f.Ref
	}
	return false
}
