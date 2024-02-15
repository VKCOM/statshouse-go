// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statshouse

import (
	"math"
	"testing"
)

func TestLexEnc(t *testing.T) {
	// from -Inf up towards 0
	lexEncTestRange(t, float32(math.Inf(-1)), 0, 1000)
	// through 0
	lexEncTestSlice(t, []float32{-1, 0, 1}, nil)
	lexEncTestRange(t, -math.SmallestNonzeroFloat32, math.SmallestNonzeroFloat32, math.MaxInt64)
	// from +Inf down towards 0
	lexEncTestRange(t, float32(math.Inf(+1)), 0, 1000)
	// NaN
	nan := LexDecode(LexEncode(float32(math.NaN())))
	if !math.IsNaN(float64(nan)) {
		t.Error("nan is not nan")
	}
}

func lexEncTestRange(t *testing.T, first, last float32, cnt int64) {
	var (
		src     = []float32{first, math.Nextafter32(first, last)}
		enc     = []int32{0, 0}
		bounded = cnt < math.MaxInt64
	)
	for ; src[0] != src[1] && 0 < cnt; cnt-- {
		lexEncTestSlice(t, src, enc)
		src[0] = src[1]
		src[1] = math.Nextafter32(src[0], last)
	}
	if bounded && cnt != 0 {
		t.Error("cnt must be 0")
	}
}

func lexEncTestSlice(t *testing.T, src []float32, enc []int32) {
	if enc == nil {
		enc = make([]int32, len(src))
	}
	for i := range src {
		enc[i] = LexEncode(src[i])
		if LexDecode(enc[i]) != src[i] {
			t.Error("not equal")
		}
	}
	for i := 1; i < len(enc); i++ {
		if src[i-1] < src[i] {
			if enc[i-1] >= enc[i] {
				t.Error("wrong order")
			}
		} else {
			if enc[i-1] <= enc[i] {
				t.Error("wrong order")
			}
		}
	}
}
