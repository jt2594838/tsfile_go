package operator

import "strings"

type IntLtFilter struct {
	ref int32
}

func (f *IntLtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return v < f.ref
	}
	return false
}

type LongLtFilter struct {
	ref int64
}

func (f *LongLtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return v < f.ref
	}
	return false
}

type StrLtFilter struct {
	ref string
}

func (f *StrLtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.ref) < 0
	}
	return false
}

type FloatLtFilter struct {
	ref float32
}

func (f *FloatLtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v < f.ref
	}
	return false
}

type DoubleLtFilter struct {
	ref float64
}

func (f *DoubleLtFilter) satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v < f.ref
	}
	return false
}

