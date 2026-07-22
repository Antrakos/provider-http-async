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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Polling declares how to poll a long-running operation to completion after a
// mutating mapping (CREATE/UPDATE/DELETE) fires. When absent, the mapping behaves
// synchronously (identical to provider-http).
type Polling struct {
	// URL is a jq expression evaluated against .response (the mutate response) to
	// produce the operation URL to poll. Stable for the entire loop.
	URL string `json:"url"`

	// Done is a jq expression evaluated against .poll.response each iteration.
	// Must return a boolean. When true the loop stops.
	Done string `json:"done"`

	// Error is a jq expression evaluated against .poll.response after Done is true.
	// A non-null result is a terminal failure; its value is surfaced verbatim in
	// the resource condition.
	// +optional
	Error string `json:"error,omitempty"`

	// Timeout bounds the whole poll loop. Defaults to 30m.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// Interval is the delay between poll iterations. Defaults to 5s.
	// +optional
	Interval *metav1.Duration `json:"interval,omitempty"`
}
