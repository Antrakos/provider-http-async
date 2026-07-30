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
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

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

// RawExtensionToMap deserializes a runtime.RawExtension holding a JSON object into the
// raw map used as .response in the poll jq context. It returns nil when ext is nil or
// has no raw bytes, so callers can treat a nil result as "no in-flight operation".
func RawExtensionToMap(ext *runtime.RawExtension) map[string]interface{} {
	if ext == nil || len(ext.Raw) == 0 {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(ext.Raw, &m); err != nil {
		return nil
	}
	return m
}

// StructToMap serializes any JSON-marshalable value to a JSON-compatible map, so jq
// expressions can key off the whole struct (e.g. the full status). It lives here rather
// than in internal/json to avoid a test-build import cycle (internal/json's tests import
// the API packages). Returns nil on a marshal/unmarshal error (a programming error).
func StructToMap(obj interface{}) map[string]interface{} {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil
	}
	return m
}
