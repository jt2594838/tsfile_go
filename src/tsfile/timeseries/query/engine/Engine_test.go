package engine

import (
	"fmt"
	"testing"
	"tsfile/timeseries/filter"
	"tsfile/timeseries/filter/operator"
	"tsfile/timeseries/query"
	"tsfile/timeseries/read"
)

var tempFilePath = "temp_TsFile"
var series = []string{"root.d0.s0", "root.d0.s1", "root.d1.s0"}

func prepareTsFile() {
	/*
		Assumed data layout:
		root.d0.s0 : [1,1], [2,2], [3,3], [4,4], [5,5]
		root.d0.s1 : [1,5], [2,4],        [4,3], [5,2], [6,1]
		root.d1.s0 :               [3,3], [4,4], [5,5]
	*/
}

func TestEngine(t *testing.T) {

	prepareTsFile()

	f := new(read.TsFileSequenceReader)
	f.Open(tempFilePath)
	engine := new(Engine)
	engine.Open(f)
	defer func() {
		engine.Close()
		f.Close()
	}()

	// test a non-existing series
	paths := []string{"not a series"}
	exp := new(query.QueryExpression)
	exp.SetSelectPaths(paths)
	dataSet := engine.Query(exp)
	if dataSet.HasNext() {
		t.Fatal("This timeseries should not present in this file")
	}

	// test selecting a series without conditions
	paths = []string{series[0]}
	exp.SetSelectPaths(paths)
	dataSet = engine.Query(exp)
	cnt := int32(0)
	for dataSet.HasNext() {
		record := dataSet.Next()
		cnt++
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt) || record.Values()[0].(int32) != cnt {
			t.Fatal(fmt.Sprintf("Expected [%d, %d] got %v", cnt, cnt, record))
		}
	}

	// test an existing series but no value satisfies the given condition
	paths = []string{series[0]}
	var filt filter.Filter = filter.NewRowRecordValFilter(series[0], &operator.IntGtFilter{5})
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	if dataSet.HasNext() {
		t.Fatal("This timeseries should not have any value > 5")
	}

	// test an existing series with some satisfying values
	paths = []string{series[0]}
	filt = filter.NewRowRecordValFilter(series[0], &operator.IntLtEqFilter{3})
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	for dataSet.HasNext() {
		record := dataSet.Next()
		cnt++
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt) || record.Values()[0].(int32) != cnt {
			t.Fatal(fmt.Sprintf("Expected [%d, %d] got %v", cnt, cnt, record))
		}
	}
	if cnt > 3 {
		t.Fatal(fmt.Sprintf("More values than expected are returned, expected 3 got %d", cnt))
	}

	// test selecting multiple series without conditions
	paths = []string{series[0], series[1]}
	exp.SetSelectPaths(paths)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	s0Vals := []int32{1, 2, 3, 4, 5, nil}
	s1Vals := []int32{5, 4, nil, 3, 2, 1}
	for dataSet.HasNext() {
		record := dataSet.Next()
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt+1) ||
			record.Values()[0].(int32) != s0Vals[cnt] ||
			record.Values()[1].(int32) != s1Vals[cnt] {
			t.Fatal(fmt.Sprintf("Expected [%d, %d, %d] got %v", cnt+1, s0Vals[cnt], s1Vals[cnt], record))
		}
		cnt++
	}

	// test selecting multiple series with conditions that can't be satisfied
	paths = []string{series[0], series[1]}
	filt = &filter.RowRecordTimeFilter{&operator.LongGtEqFilter{10}}
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	if dataSet.HasNext() {
		t.Fatal("This timeseries should not have any time > 10")
	}

	// test selecting multiple series with satisfiable conditions
	// and the condition path is among the select paths
	paths = []string{series[0], series[1]}
	filt = filter.NewRowRecordValFilter(series[0], &operator.IntGtEqFilter{4})
	exp.SetSelectPaths(paths)
	exp.SetConditionPaths([]string{series[0]})
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	s0Vals = []int32{4, 5}
	s1Vals = []int32{3, 2}
	for dataSet.HasNext() {
		record := dataSet.Next()
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt+4) ||
			record.Values()[0].(int32) != s0Vals[cnt] ||
			record.Values()[1].(int32) != s1Vals[cnt] {
			t.Fatal(fmt.Sprintf("Expected [%d, %d, %d] got %v", cnt+4, s0Vals[cnt], s1Vals[cnt], record))
		}
		cnt++
	}

	// test selecting multiple series with satisfiable conditions
	// and the condition path is outside the select paths
	paths = []string{series[0], series[1]}
	filt = filter.NewRowRecordValFilter(series[2], &operator.IntGtEqFilter{4})
	exp.SetConditionPaths([]string{series[2]})
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	s0Vals = []int32{4, 5}
	s1Vals = []int32{3, 2}
	for dataSet.HasNext() {
		record := dataSet.Next()
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt+4) ||
			record.Values()[0].(int32) != s0Vals[cnt] ||
			record.Values()[1].(int32) != s1Vals[cnt] {
			t.Fatal(fmt.Sprintf("Expected [%d, %d, %d] got %v", cnt+4, s0Vals[cnt], s1Vals[cnt], record))
		}
		cnt++
	}

	// test selecting multiple series with satisfiable conditions
	// and the condition paths share some common paths with the select paths
	paths = []string{series[0], series[1]}
	filt = &operator.AndFilter{[]filter.Filter{filter.NewRowRecordValFilter(series[2], &operator.IntGtEqFilter{4}),
		filter.NewRowRecordValFilter(series[1], &operator.IntGtEqFilter{3})}}

	exp.SetConditionPaths([]string{series[2]})
	exp.SetSelectPaths(paths)
	exp.SetFilter(filt)
	dataSet = engine.Query(exp)
	cnt = int32(0)
	s0Vals = []int32{4}
	s1Vals = []int32{3}
	for dataSet.HasNext() {
		record := dataSet.Next()
		checkPath(paths, record.Paths(), t)
		if record.Timestamp() != int64(cnt+4) ||
			record.Values()[0].(int32) != s0Vals[cnt] ||
			record.Values()[1].(int32) != s1Vals[cnt] {
			t.Fatal(fmt.Sprintf("Expected [%d, %d, %d] got %v", cnt+4, s0Vals[cnt], s1Vals[cnt], record))
		}
		cnt++
	}
}

func checkPath(pathA []string, pathB []string, t *testing.T) {
	if len(pathA) != len(pathB) {
		t.Fatal("SelectPaths not consistent")
	}
	for i, _ := range pathA {
		if pathA[i] != pathB[i] {
			t.Fatal("SelectPaths not consistent")
		}
	}
}
