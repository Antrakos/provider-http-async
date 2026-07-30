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

const (
	ExpectedResponseCheckTypeDefault = "DEFAULT"
	ExpectedResponseCheckTypeCustom  = "CUSTOM"
)

const (
	ActionCreate  = "CREATE"
	ActionObserve = "OBSERVE"
	ActionUpdate  = "UPDATE"
	ActionRemove  = "REMOVE"
)

// AsyncRequestParameters are the configurable fields of an AsyncRequest.
// +kubebuilder:validation:XValidation:rule="!(self.insecureSkipTLSVerify == true && has(self.tlsConfig))",message="insecureSkipTLSVerify and tlsConfig are mutually exclusive"
// +kubebuilder:validation:XValidation:rule="!(self.mappings.exists(m, has(m.polling)) && self.mappings.exists(m, ((has(m.action) && m.action == 'OBSERVE') || (!has(m.action) && has(m.method) && m.method == 'GET')) && m.url.contains('.response') && !m.url.contains('.status.response') && !m.url.contains('.status.externalRef')))",message="polling is configured but the OBSERVE URL derives identity from the mutate response (.response); a polled resource must key its OBSERVE URL on .status.externalRef (set spec.externalRef to extract it) or use a constant URL"
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

	// ResourceExistsCheck optionally decouples existence detection from drift
	// detection for a sub-resource embedded in a parent OBSERVE response that always
	// returns 2xx (e.g. a Vertex AI deployedModel observed via its parent endpoint,
	// which returns 200 whether or not any model is deployed to it).
	//
	// It is a CUSTOM jq expression evaluated against the OBSERVE response (with
	// .status.externalRef available), after the in-flight anchor gate and before
	// expectedResponseCheck. When it returns false the reconciler creates the
	// resource; when true, expectedResponseCheck determines drift.
	//
	// The DEFAULT type (and an unset field) keep the default behavior: existence is
	// inferred from the OBSERVE HTTP status. A non-2xx response on a first observe
	// (no externalRef, no prior response) already routes to CREATE, and an
	// isRemovedCheck 404 routes to delete — both fire before this check, so
	// resourceExistsCheck only applies on a 2xx response, which is precisely the
	// case the default inference cannot answer (the parent's 200 does not tell you
	// whether the sub-resource you own is present). There is no DEFAULT jq logic
	// because the default is not a jq expression — it is the HTTP-status inference
	// above. This field is therefore only meaningful with type CUSTOM.
	ResourceExistsCheck ExpectedResponseCheck `json:"resourceExistsCheck,omitempty"`

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

	// IsTerminalError is a jq expression that classifies a failure as terminal
	// (stall until a spec change) versus retryable (requeue and re-fire, the
	// default). Evaluated against the same context as externalRef — {spec, status,
	// response, poll} — after a mutate response (CREATE/UPDATE/DELETE, polling or
	// not) or a poll response that the poll loop reported as failed. It is NOT
	// evaluated on the OBSERVE existence-check response.
	//
	// Resolves to string or boolean, following the externalRef pattern:
	//   - non-empty string → terminal; the string is the surfaced message
	//     (status.terminalError + Ready=False with a "Terminal error: " prefix);
	//   - true            → terminal with a provider-authored default message;
	//   - empty / false / null → retry (the default).
	//
	// Unset (empty) is the base provider-http behavior generalized to polling:
	// every failure requeues and re-fires, except a polling.timeout (operation
	// state unknown), which is terminated by the provider regardless. Terminal is
	// strictly opt-in; the full .status (not just .status.externalRef) is available
	// in the context, so e.g. `.status.failed > 5` yields a bounded-retry policy.
	// +optional
	IsTerminalError string `json:"isTerminalError,omitempty"`

	// OIDC overrides the ProviderConfig OIDC settings for this resource.
	// +optional
	OIDC *common.OIDCConfig `json:"oidc,omitempty"`

	// GCP overrides the ProviderConfig GCP auth settings for this resource.
	// Field-merge with the ProviderConfig defaults via MergeGCPConfigs: a
	// non-empty override value wins, scopes replace.
	// +optional
	GCP *common.GCPAuth `json:"gcp,omitempty"`
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
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              AsyncRequestParameters `json:"forProvider"`
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
	Response                   Response `json:"response,omitempty"`
	Cache                      Cache    `json:"cache,omitempty"`
	Failed                     int32    `json:"failed,omitempty"`
	Error                      string   `json:"error,omitempty"`
	RequestDetails             Mapping  `json:"requestDetails,omitempty"`

	// ExternalRef is the stable external identifier extracted after the first
	// successful CREATE+poll cycle (or seeded from crossplane.io/external-name on
	// import). Used by OBSERVE/UPDATE/DELETE URLs via .status.externalRef.
	// +optional
	ExternalRef string `json:"externalRef,omitempty"`

	// TerminalError holds the message from a terminal failure — a failure classified as
	// not self-healing, so the controller stalls until a spec change. It is no longer
	// polling-specific: it is written by the user's spec.isTerminalError jq expression
	// (evaluated on a mutate response or a poll-failure response) and by provider-authored
	// terminals (polling.url bad/empty, polling.timeout, drift with no UPDATE mapping). The
	// message is the user expression's value coerced to a string (a structured object/array
	// is JSON-marshaled, a scalar is fmt.Sprint'd) or a provider-authored string. While set
	// and observedGeneration == generation the controller reports the resource unhealthy
	// (Ready=False, "Terminal error: " prefix) and does NOT re-fire the mutate call. A spec
	// change bumps the generation; IsUpToDate detects the drift, clears terminalError, and
	// re-evaluates. The full failing response is surfaced in status.response (same as base
	// provider-http); terminalError carries only the message.
	// +optional
	TerminalError string `json:"terminalError,omitempty"`

	// Polling holds state for the in-flight long-running operation poll loop.
	// +optional
	Polling PollingStatus `json:"polling,omitempty"`
}

// PollingStatus groups the fields that track an in-flight long-running operation.
// Response (the crash-recovery anchor) is set when the mutate call succeeds and cleared
// only when the operation completes or its anchor policy fires; StartedAt tracks its
// lifetime. Terminal failures no longer live here — see AsyncRequestStatus.TerminalError.
type PollingStatus struct {
	// Response is the raw response from the mutate call (CREATE/UPDATE/DELETE) whose
	// long-running operation is being polled. It is the crash-recovery anchor: while
	// non-null, a reconcile resumes polling (recomputing polling.url against this
	// response) instead of re-firing the mutate call. Set when the mutate call succeeds
	// and cleared when the operation completes or its cause-dependent anchor policy
	// fires. Retained across a terminal polling.timeout / bad-polling.url so a corrected
	// spec resumes the existing operation rather than creating a duplicate; cleared on an
	// operation failure (polling.error non-null) since the operation is dead and a retry
	// re-fires a fresh, duplicate-safe mutate.
	// +optional
	Response *runtime.RawExtension `json:"response,omitempty"`

	// StartedAt is the time polling began. Combined with polling.timeout it gives
	// the absolute deadline across all reconciles, preventing the timeout from
	// resetting on every requeue. Cleared together with the anchor on operation
	// completion or an operation-failure anchor clear; retained across a
	// timeout/config terminal so a corrected spec resumes within the same budget.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
}

type Cache struct {
	LastUpdated string   `json:"lastUpdated,omitempty"`
	Response    Response `json:"response,omitempty"`
}

// +kubebuilder:object:root=true

// A AsyncRequest is a namespaced HTTP request resource.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="EXTERNAL-REF",type="string",JSONPath=".status.externalRef"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,http}
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
