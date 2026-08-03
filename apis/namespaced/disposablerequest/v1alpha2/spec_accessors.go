/*
Copyright 2022 The Crossplane Authors.

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
	"github.com/Antrakos/provider-http-async/apis/interfaces"
)

// Ensure AsyncDisposableRequestParameters implements SimpleHTTPRequestSpec
var _ interfaces.SimpleHTTPRequestSpec = (*AsyncDisposableRequestParameters)(nil)

// Ensure AsyncDisposableRequestParameters implements ReconciliationPolicyAware
var _ interfaces.ReconciliationPolicyAware = (*AsyncDisposableRequestParameters)(nil)

// Ensure AsyncDisposableRequestParameters implements RollbackAware
var _ interfaces.RollbackAware = (*AsyncDisposableRequestParameters)(nil)

// GetWaitTimeout returns the maximum time duration for waiting.
func (d *AsyncDisposableRequestParameters) GetWaitTimeout() *metav1.Duration {
	return d.WaitTimeout
}

// GetInsecureSkipTLSVerify returns whether to skip TLS certificate verification.
func (d *AsyncDisposableRequestParameters) GetInsecureSkipTLSVerify() bool {
	return d.InsecureSkipTLSVerify
}

// GetSecretInjectionConfigs returns the secret injection configurations.
func (d *AsyncDisposableRequestParameters) GetSecretInjectionConfigs() []common.SecretInjectionConfig {
	return d.SecretInjectionConfigs
}

// GetHeaders returns the default headers for the request.
func (d *AsyncDisposableRequestParameters) GetHeaders() map[string][]string {
	return d.Headers
}

// GetURL returns the URL for the request.
func (d *AsyncDisposableRequestParameters) GetURL() string {
	return d.URL
}

// GetMethod returns the HTTP method for the request.
func (d *AsyncDisposableRequestParameters) GetMethod() string {
	return d.Method
}

// GetBody returns the body of the request.
func (d *AsyncDisposableRequestParameters) GetBody() string {
	return d.Body
}

// GetExpectedResponse returns the jq filter expression for validating the response.
func (d *AsyncDisposableRequestParameters) GetExpectedResponse() string {
	return d.ExpectedResponse
}

// GetNextReconcile returns the duration after which the next reconcile should occur.
func (d *AsyncDisposableRequestParameters) GetNextReconcile() *metav1.Duration {
	return d.NextReconcile
}

// GetShouldLoopInfinitely returns whether reconciliation should loop indefinitely.
func (d *AsyncDisposableRequestParameters) GetShouldLoopInfinitely() bool {
	return d.ShouldLoopInfinitely
}

// GetRollbackRetriesLimit returns the maximum number of rollback retry attempts.
func (d *AsyncDisposableRequestParameters) GetRollbackRetriesLimit() *int32 {
	return d.RollbackRetriesLimit
}

// GetAllowedStatusCodes returns the HTTP status codes that should not be treated as errors.
func (d *AsyncDisposableRequestParameters) GetAllowedStatusCodes() []int {
	return d.AllowedStatusCodes
}

// Ensure Response implements HTTPResponse
var _ interfaces.HTTPResponse = (*Response)(nil)

// GetStatusCode returns the HTTP status code.
func (r *Response) GetStatusCode() int {
	return r.StatusCode
}

// GetBody returns the response body.
func (r *Response) GetBody() string {
	return r.Body
}

// GetHeaders returns the response headers.
func (r *Response) GetHeaders() map[string][]string {
	return r.Headers
}

// Ensure AsyncDisposableRequest implements CachedResponse
var _ interfaces.CachedResponse = (*AsyncDisposableRequest)(nil)

// GetCachedResponse returns the cached response from the status.
func (d *AsyncDisposableRequest) GetCachedResponse() interfaces.HTTPResponse {
	if d.Status.Response.StatusCode == 0 {
		return nil
	}
	return &d.Status.Response
}

// GetSynced returns whether the resource is synced.
func (d *AsyncDisposableRequest) GetSynced() bool {
	return d.Status.Synced
}

// GetFailed returns the failure count.
func (d *AsyncDisposableRequest) GetFailed() int32 {
	return d.Status.Failed
}

// GetResponse returns the HTTP response from status.
func (d *AsyncDisposableRequest) GetResponse() interfaces.HTTPResponse {
	return &d.Status.Response
}

// SetFailed sets the failure count.
func (d *AsyncDisposableRequest) SetFailed(failed int32) {
	d.Status.Failed = failed
}

// Ensure AsyncDisposableRequest implements DisposableRequestStatus
var _ interfaces.DisposableRequestStatus = (*AsyncDisposableRequest)(nil)

// Ensure AsyncDisposableRequest implements DisposableRequestResource
var _ interfaces.DisposableRequestResource = (*AsyncDisposableRequest)(nil)

// GetSpec returns the request specification (ForProvider parameters).
func (d *AsyncDisposableRequest) GetSpec() interfaces.SimpleHTTPRequestSpec {
	return &d.Spec.ForProvider
}
