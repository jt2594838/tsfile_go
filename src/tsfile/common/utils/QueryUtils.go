package utils

import (
	"sort"
	"strings"
)

// MergeStrings sorts and merges two string lists in ascent order
func MergeStrings(strsA []string, strsB []string) []string {
	if strsB == nil {
		return strsA
	} else if strsA == nil {
		return strsB
	}

	lenA, lenB := len(strsA), len(strsB)
	indexA, indexB := 0, 0
	sort.Strings(strsA)
	sort.Strings(strsB)
	ret := make([]string, lenA + lenB)
	for indexA < lenA && indexB < lenB{
		order := strings.Compare(strsA[indexA], strsB[indexB])
		if order == 0 {
			ret = append(ret, strsA[indexA])
			indexA ++
			indexB ++
		} else if order < 0{
			ret = append(ret, strsA[indexA])
			indexA ++
		} else {
			ret = append(ret, strsB[indexB])
			indexB ++
		}
	}
	for indexA < lenA {
		ret = append(ret, strsA[indexA])
		indexA ++
	}
	for indexB < lenB {
		ret = append(ret, strsB[indexB])
		indexB ++
	}
	return ret
}

// TestCommonStrs tests if two string slices have common strings using sort and merge
func TestCommonStrs(strsA []string, strsB []string) bool {
	if strsB == nil || strsA == nil {
		return false
	}
	lenA, lenB := len(strsA), len(strsB)
	indexA, indexB := 0, 0
	sort.Strings(strsA)
	sort.Strings(strsB)
	for indexA < lenA && indexB < lenB{
		order := strings.Compare(strsA[indexA], strsB[indexB])
		if order == 0 {
			return true
		} else if order < 0{
			indexA ++
		} else {
			indexB ++
		}
	}
	return false
}
