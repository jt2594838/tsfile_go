/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package operator

import "strings"

// LtFilters compare the input value to the Reference value, and return true iff the input < the Reference.
// Type mismatch will set the return value to false. Use lexicographical order for strings.
// Supported types: int32(int) int64(long) float32(float) float64(double) string.
type IntLtFilter struct {
	Ref int32
}

func (f *IntLtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int32); ok {
		return v < f.Ref
	}
	return false
}

type LongLtFilter struct {
	Ref int64
}

func (f *LongLtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(int64); ok {
		return v < f.Ref
	}
	return false
}

type StrLtFilter struct {
	Ref string
}

func (f *StrLtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(string); ok {
		return strings.Compare(v, f.Ref) < 0
	}
	return false
}

type FloatLtFilter struct {
	Ref float32
}

func (f *FloatLtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float32); ok {
		return v < f.Ref
	}
	return false
}

type DoubleLtFilter struct {
	Ref float64
}

func (f *DoubleLtFilter) Satisfy(val interface{}) bool {
	if v, ok := val.(float64); ok {
		return v < f.Ref
	}
	return false
}
