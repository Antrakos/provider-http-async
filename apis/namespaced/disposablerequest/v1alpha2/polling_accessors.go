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

package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Antrakos/provider-http-async/apis/common"
)

// GetPolling returns the polling configuration for the request, or nil when the
// request is a plain (non-LRO) one-off.
func (d *AsyncDisposableRequest) GetPolling() *common.Polling {
	return d.Spec.ForProvider.Polling
}

// GetPollingResponse returns the raw trigger response anchoring an in-flight
// long-running operation, or nil when no operation is in flight.
func (d *AsyncDisposableRequest) GetPollingResponse() map[string]interface{} {
	return common.RawExtensionToMap(d.Status.Polling.Response)
}

// SetPollingResponse persists (or clears, when passed nil) the raw trigger response
// that anchors an in-flight long-running operation in status.polling.response.
func (d *AsyncDisposableRequest) SetPollingResponse(m map[string]interface{}) {
	d.Status.Polling.Response = common.MapToRawExtension(m)
}

// GetOperationStartedAt returns the time the in-flight operation began polling, or nil.
func (d *AsyncDisposableRequest) GetOperationStartedAt() *metav1.Time {
	return d.Status.Polling.StartedAt
}

// SetOperationStartedAt records the time the in-flight operation began polling.
// Pass nil to clear it.
func (d *AsyncDisposableRequest) SetOperationStartedAt(t *metav1.Time) {
	d.Status.Polling.StartedAt = t
}
