/*
Copyright 2024 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"testing"
	"time"
)

func strPtr(s string) *string { return &s }

func TestParseDuration(t *testing.T) {
	const def = 30 * time.Minute

	cases := []struct {
		name string
		in   *string
		want time.Duration
	}{
		{name: "nil returns default", in: nil, want: def},
		{name: "empty returns default", in: strPtr(""), want: def},
		{name: "unparseable returns default", in: strPtr("30min"), want: def},
		{name: "bare number returns default", in: strPtr("30"), want: def},
		{name: "valid literal round-trips", in: strPtr("60m"), want: 60 * time.Minute},
		{name: "compound literal", in: strPtr("1h30m"), want: 90 * time.Minute},
		{name: "seconds", in: strPtr("5s"), want: 5 * time.Second},
		{name: "negative parses (no validation here)", in: strPtr("-5m"), want: -5 * time.Minute},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseDuration(tc.in, def); got != tc.want {
				t.Errorf("ParseDuration(%v, %v) = %v, want %v", tc.in, def, got, tc.want)
			}
		})
	}
}
