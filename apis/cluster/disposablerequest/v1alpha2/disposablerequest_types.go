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
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/Antrakos/provider-http-async/apis/common"
)

// AsyncDisposableRequestParameters are the configurable fields of an AsyncDisposableRequest.
// +kubebuilder:validation:XValidation:rule="!(self.insecureSkipTLSVerify == true && has(self.tlsConfig))",message="insecureSkipTLSVerify and tlsConfig are mutually exclusive"
type AsyncDisposableRequestParameters struct {
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Field 'forProvider.url' is immutable"
	URL string `json:"url"`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Field 'forProvider.method' is immutable"
	Method string `json:"method"`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Field 'forProvider.headers' is immutable"
	Headers map[string][]string `json:"headers,omitempty"`
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="Field 'forProvider.body' is immutable"
	Body string `json:"body,omitempty"`

	// WaitTimeout specifies the maximum time duration for waiting, as a Go duration string (e.g. "5m").
	WaitTimeout *string `json:"waitTimeout,omitempty"`

	// RollbackRetriesLimit caps how many times the request is attempted; the first
	// attempt counts, so set it to 1 for a strict one-off (fire once, and on failure
	// stop and surface ErrorObserved). Leave unset to keep retrying until the response
	// matches. Must be >= 1; 0 is rejected because it would stop the request from ever firing.
	// +kubebuilder:validation:Minimum=1
	RollbackRetriesLimit *int32 `json:"rollbackRetriesLimit,omitempty"`

	// InsecureSkipTLSVerify, when set to true, skips TLS certificate checks for the HTTP request.
	// This field is mutually exclusive with TLSConfig.
	// +optional
	InsecureSkipTLSVerify bool `json:"insecureSkipTLSVerify,omitempty"`

	// TLSConfig allows overriding the TLS configuration from ProviderConfig for this specific request.
	// This field is mutually exclusive with InsecureSkipTLSVerify.
	// +optional
	TLSConfig *common.TLSConfig `json:"tlsConfig,omitempty"`

	// GCP configures Google Cloud authentication for the request, allowing the
	// provider to obtain and attach a bearer token (optionally via service account
	// impersonation). Mutually exclusive with OIDC.
	// +optional
	GCP *common.GCPAuth `json:"gcp,omitempty"`

	// OIDC configures an RFC 8693 token exchange so the provider attaches a
	// short-lived bearer token to the request. Mutually exclusive with GCP.
	// +optional
	OIDC *common.OIDCConfig `json:"oidc,omitempty"`

	// ExpectedResponse is a jq filter expression used to evaluate the HTTP response and determine if it matches the expected criteria.
	// When polling is configured it is evaluated against the final poll response; otherwise against the request response.
	// The expression should return a boolean; if true, the response is considered expected.
	// Example: '.body.job_status == "success"'
	ExpectedResponse string `json:"expectedResponse,omitempty"`

	// Polling turns the single request into a long-running-operation (LRO) driver.
	// When set, the initial request is treated as the operation trigger and the
	// provider polls polling.url until polling.done is true (or polling.error is
	// non-null), then evaluates ExpectedResponse against the final poll response.
	// +optional
	Polling *common.Polling `json:"polling,omitempty"`

	// NextReconcile specifies the duration after which the next reconcile should occur, as a Go duration string (e.g. "1h").
	NextReconcile *string `json:"nextReconcile,omitempty"`

	// ShouldLoopInfinitely specifies whether the reconciliation should loop indefinitely.
	ShouldLoopInfinitely bool `json:"shouldLoopInfinitely,omitempty"`

	// SecretInjectionConfig specifies the secrets receiving patches from response data.
	SecretInjectionConfigs []common.SecretInjectionConfig `json:"secretInjectionConfigs,omitempty"`

	// AllowedStatusCodes specifies HTTP status codes that should not be treated as errors.
	// By default, status codes 400-599 are considered errors. This field allows users to
	// override that behavior for specific status codes (e.g., treating 404 as valid).
	// +optional
	AllowedStatusCodes []int `json:"allowedStatusCodes,omitempty"`
}

// A AsyncDisposableRequestSpec defines the desired state of an AsyncDisposableRequest.
type AsyncDisposableRequestSpec struct {
	xpv2.ClusterManagedResourceSpec `json:",inline"`
	ForProvider                     AsyncDisposableRequestParameters `json:"forProvider"`
}

type Response struct {
	StatusCode int                 `json:"statusCode,omitempty"`
	Body       string              `json:"body,omitempty"`
	Headers    map[string][]string `json:"headers,omitempty"`
}

type Mapping struct {
	Method  string              `json:"method"`
	Body    string              `json:"body,omitempty"`
	URL     string              `json:"url"`
	Headers map[string][]string `json:"headers,omitempty"`
}

// A AsyncDisposableRequestStatus represents the observed state of an AsyncDisposableRequest.
type AsyncDisposableRequestStatus struct {
	xpv2.ManagedResourceStatus `json:",inline"`
	Response                   Response `json:"response,omitempty"`
	Failed                     int32    `json:"failed,omitempty"`
	Error                      string   `json:"error,omitempty"`
	Synced                     bool     `json:"synced,omitempty"`
	RequestDetails             Mapping  `json:"requestDetails,omitempty"`

	// Polling holds state for the in-flight long-running operation poll loop.
	// +optional
	Polling PollingStatus `json:"polling,omitempty"`

	// LastReconcileTime records the last time the resource was reconciled.
	LastReconcileTime metav1.Time `json:"lastReconcileTime,omitempty"`
}

// PollingStatus groups the fields that track an in-flight long-running operation.
type PollingStatus struct {
	// Response is the raw response from the initial request whose long-running
	// operation is being polled. It is the crash-recovery anchor: while non-null a
	// reconcile resumes polling (recomputing polling.url against this response)
	// instead of re-firing the request. Cleared when the operation completes.
	// +optional
	Response *runtime.RawExtension `json:"response,omitempty"`

	// StartedAt is the time polling began. Combined with polling.timeout it gives
	// the absolute deadline across all reconciles.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
}

// +kubebuilder:object:root=true

// A AsyncDisposableRequest is a one-off HTTP request managed resource.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,categories={crossplane,managed,http}
// +kubebuilder:storageversion
type AsyncDisposableRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AsyncDisposableRequestSpec   `json:"spec"`
	Status AsyncDisposableRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AsyncDisposableRequestList contains a list of AsyncDisposableRequest
type AsyncDisposableRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AsyncDisposableRequest `json:"items"`
}

// AsyncDisposableRequest type metadata.
var (
	AsyncDisposableRequestKind             = reflect.TypeOf(AsyncDisposableRequest{}).Name()
	AsyncDisposableRequestGroupKind        = schema.GroupKind{Group: Group, Kind: AsyncDisposableRequestKind}.String()
	AsyncDisposableRequestKindAPIVersion   = AsyncDisposableRequestKind + "." + SchemeGroupVersion.String()
	AsyncDisposableRequestGroupVersionKind = SchemeGroupVersion.WithKind(AsyncDisposableRequestKind)
)

func init() {
	SchemeBuilder.Register(&AsyncDisposableRequest{}, &AsyncDisposableRequestList{})
}
