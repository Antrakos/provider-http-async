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

import "time"

// ParseDuration parses a Go duration string (e.g. "30m"), returning def when s is
// nil, empty, or unparseable. Duration-typed spec fields are plain strings (not
// metav1.Duration) so the exact user-supplied text round-trips through the API
// server unchanged — metav1.Duration always re-marshals to its canonical
// time.Duration.String() form (e.g. "60m" becomes "1h0m0s"), which fights any
// controller (Crossplane composition, kubectl apply, ...) that re-applies the
// original literal, bumping metadata.generation forever.
func ParseDuration(s *string, def time.Duration) time.Duration {
	if s == nil || *s == "" {
		return def
	}
	d, err := time.ParseDuration(*s)
	if err != nil {
		return def
	}
	return d
}
