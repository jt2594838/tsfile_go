package operator

import "strings"

type IntLtEqFilter struct {
	ref int32
}

func (f *IntLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return v <= f.ref
	}
	return false
}

type LongLtEqFilter struct {
	ref int64
}

func (f *LongLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return v <= f.ref
	}
	return false
}

type StrLtEqFilter struct {
	ref string
}

func (f *StrLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.ref) <= 0
	}
	return false
}

type FloatLtEqFilter struct {
	ref float32
}

func (f *FloatLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v <= f.ref
	}
	return false
}

type DoubleLtEqFilter struct {
	ref float64
}

func (f *DoubleLtEqFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v <= f.ref
	}
	return false
}

