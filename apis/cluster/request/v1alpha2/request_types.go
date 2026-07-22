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
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/Antrakos/provider-http-async/apis/common"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
)

// Re-export common constants for backward compatibility
const (
	ExpectedResponseCheckTypeDefault = common.ExpectedResponseCheckTypeDefault
	ExpectedResponseCheckTypeCustom  = common.ExpectedResponseCheckTypeCustom
)

const (
	ActionCreate  = common.ActionCreate
	ActionObserve = common.ActionObserve
	ActionUpdate  = common.ActionUpdate
	ActionRemove  = common.ActionRemove
)

// AsyncRequestParameters are the configurable fields of an AsyncRequest.
// +kubebuilder:validation:XValidation:rule="!(self.insecureSkipTLSVerify == true && has(self.tlsConfig))",message="insecureSkipTLSVerify and tlsConfig are mutually exclusive"
type AsyncRequestParameters struct {
	// Mappings defines the HTTP mappings for different methods.
	// Either Method or Action must be specified. If both are omitted, the mapping will not be used.
	// +kubebuilder:validation:MinItems=1
	Mappings []Mapping `json:"mappings"`

	// Payload defines the payload for the request.
	Payload Payload `json:"payload"`

	// Headers defines default headers for each request.
	Headers map[string][]string `json:"headers,omitempty"`

	// WaitTimeout specifies the maximum time duration for waiting.
	WaitTimeout *metav1.Duration `json:"waitTimeout,omitempty"`

	// InsecureSkipTLSVerify, when set to true, skips TLS certificate checks for the HTTP request.
	// This field is mutually exclusive with TLSConfig.
	// +optional
	InsecureSkipTLSVerify bool `json:"insecureSkipTLSVerify,omitempty"`

	// TLSConfig allows overriding the TLS configuration from ProviderConfig for this specific request.
	// This field is mutually exclusive with InsecureSkipTLSVerify.
	// +optional
	TLSConfig *common.TLSConfig `json:"tlsConfig,omitempty"`

	// SecretInjectionConfig specifies the secrets receiving patches for response data.
	SecretInjectionConfigs []common.SecretInjectionConfig `json:"secretInjectionConfigs,omitempty"`

	// ExpectedResponseCheck specifies the mechanism to validate the OBSERVE response against expected value.
	ExpectedResponseCheck ExpectedResponseCheck `json:"expectedResponseCheck,omitempty"`

	// IsRemovedCheck specifies the mechanism to validate the OBSERVE response after removal against expected value.
	IsRemovedCheck ExpectedResponseCheck `json:"isRemovedCheck,omitempty"`

	// AllowedStatusCodes specifies HTTP status codes that should not be treated as errors.
	// By default, status codes 400-599 are considered errors. This field allows users to
	// override that behavior for specific status codes (e.g., treating 404 as valid).
	// +optional
	AllowedStatusCodes []int `json:"allowedStatusCodes,omitempty"`

	// ExternalRef is a top-level jq expression that extracts the stable external
	// identifier. Evaluated against .poll.response first, falling back to
	// .response. The result is written to status.externalRef.
	// +optional
	ExternalRef string `json:"externalRef,omitempty"`

	// OIDC overrides the ProviderConfig OIDC settings for this resource.
	// +optional
	OIDC *common.OIDCConfig `json:"oidc,omitempty"`
}

type Mapping struct {
	// +kubebuilder:validation:Enum=POST;GET;PUT;DELETE;PATCH;HEAD;OPTIONS
	// Method specifies the HTTP method for the request.
	Method string `json:"method,omitempty"`

	// +kubebuilder:validation:Enum=CREATE;OBSERVE;UPDATE;REMOVE
	// Action specifies the intended action for the request.
	Action string `json:"action,omitempty"`

	// Body specifies the body of the request.
	Body string `json:"body,omitempty"`

	// URL specifies the URL for the request.
	URL string `json:"url"`

	// Headers specifies the headers for the request.
	Headers map[string][]string `json:"headers,omitempty"`

	// Polling declares how to poll a long-running operation produced by this
	// mapping. Only meaningful on CREATE/UPDATE/DELETE mappings.
	// +optional
	Polling *common.Polling `json:"polling,omitempty"`
}

type ExpectedResponseCheck struct {
	// Type specifies the type of the expected response check.
	// +kubebuilder:validation:Enum=DEFAULT;CUSTOM
	Type string `json:"type,omitempty"`

	// Logic specifies the custom logic for the expected response check.
	Logic string `json:"logic,omitempty"`
}

type Payload struct {
	// BaseUrl specifies the base URL for the request.
	BaseUrl string `json:"baseUrl,omitempty"`

	// Body specifies data to be used in the request body.
	Body string `json:"body,omitempty"`
}

// A AsyncRequestSpec defines the desired state of an AsyncRequest.
type AsyncRequestSpec struct {
	xpv2.ClusterManagedResourceSpec `json:",inline"`
	ForProvider       AsyncRequestParameters `json:"forProvider"`
}

// AsyncRequestObservation are the observable fields of an AsyncRequest.
type Response struct {
	StatusCode int                 `json:"statusCode,omitempty"`
	Body       string              `json:"body,omitempty"`
	Headers    map[string][]string `json:"headers,omitempty"`
}

// A AsyncRequestStatus represents the observed state of an AsyncRequest.
type AsyncRequestStatus struct {
	xpv2.ManagedResourceStatus `json:",inline"`
	Response            Response `json:"response,omitempty"`
	Cache               Cache    `json:"cache,omitempty"`
	Failed              int32    `json:"failed,omitempty"`
	Error               string   `json:"error,omitempty"`
	RequestDetails      Mapping  `json:"requestDetails,omitempty"`

	// ExternalRef is the stable external identifier extracted after the first
	// successful CREATE+poll cycle (or seeded from crossplane.io/external-name on
	// import). Used by OBSERVE/UPDATE/DELETE URLs via .status.externalRef.
	// +optional
	ExternalRef string `json:"externalRef,omitempty"`

	// Polling holds state for the in-flight long-running operation poll loop.
	// +optional
	Polling PollingStatus `json:"polling,omitempty"`
}

// PollingStatus groups the fields that track an in-flight long-running operation.
// All three fields are set together when polling begins and cleared together when it ends.
type PollingStatus struct {
	// OperationRef is the in-flight mutate operation URL. It is the crash-recovery
	// anchor: a reconcile that finds it non-empty resumes polling instead of
	// re-firing the mutate call.
	// +optional
	OperationRef string `json:"operationRef,omitempty"`

	// StartedAt is the time polling began. Combined with polling.timeout it gives
	// the absolute deadline across all reconciles, preventing the timeout from
	// resetting on every requeue.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// TerminalError holds the verbatim message from a terminal poll failure
	// (polling.error non-null, or a permanent configuration error). While set, the
	// controller reports the resource unhealthy and stops re-firing the mutate call
	// until the spec changes (tracked via observedGeneration). Cleared on recovery.
	// +optional
	TerminalError string `json:"terminalError,omitempty"`
}

type Cache struct {
	LastUpdated string   `json:"lastUpdated,omitempty"`
	Response    Response `json:"response,omitempty"`
}

// +kubebuilder:object:root=true

// A AsyncRequest is an example API type.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="EXTERNAL-REF",type="string",JSONPath=".status.externalRef"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,categories={crossplane,managed,http}
// +kubebuilder:storageversion
type AsyncRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AsyncRequestSpec   `json:"spec"`
	Status AsyncRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AsyncRequestList contains a list of AsyncRequest
type AsyncRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AsyncRequest `json:"items"`
}

// AsyncRequest type metadata.
var (
	RequestKind             = reflect.TypeOf(AsyncRequest{}).Name()
	RequestGroupKind        = schema.GroupKind{Group: Group, Kind: RequestKind}.String()
	RequestKindAPIVersion   = RequestKind + "." + SchemeGroupVersion.String()
	RequestGroupVersionKind = SchemeGroupVersion.WithKind(RequestKind)
)

func init() {
	SchemeBuilder.Register(&AsyncRequest{}, &AsyncRequestList{})
}
