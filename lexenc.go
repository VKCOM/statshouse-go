// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package statshouse

import "math"

// Advanced feature.
// Encodes float as a raw tag in a special format, used by Statshouse.
// Ordering of all float values is preserved after encoding.
// Except all NaNs map to single value which is > +inf, and both zeroes map to 0.
func LexEncode(x float32) int32 {
	if x == 0 {
		return 0 // replace -0 with +0
	}
	if math.IsNaN(float64(x)) {
		return 0x7fc00000 // replace all NaNs with single positive quiet NaN
	}
	bits := math.Float32bits(x)
	if x < 0 {
		// flip all except signbit so bigger negatives go before smaller ones
		bits ^= 0x7fffffff
	}
	return int32(bits)
}

func LexDecode(x int32) float32 {
	bits := uint32(x)
	if x < 0 {
		bits ^= 0x7fffffff
	}
	return math.Float32frombits(bits)
}
