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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	InjectTypeHeader = "header"

	// #nosec G101 -- well-known mount path, not a credential
	DefaultServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

	DefaultPollTimeout       = 30 * time.Minute
	DefaultPollInterval      = 5 * time.Second
	DefaultOIDCRefreshBefore = 5 * time.Minute
)

// OIDCConfig configures transparent workload-identity token exchange for
// outgoing HTTP calls. Tokens are resolved at call time from the pod's projected
// service account token; no credentials are stored in etcd.
type OIDCConfig struct {
	// ServiceAccountTokenPath is the path to the projected SA token volume.
	// Defaults to /var/run/secrets/kubernetes.io/serviceaccount/token.
	// +optional
	ServiceAccountTokenPath string `json:"serviceAccountTokenPath,omitempty"`

	// Exchange configures the OIDC token exchange endpoint.
	// +optional
	Exchange *OIDCExchange `json:"exchange,omitempty"`

	// Inject controls how the resolved token is attached to outgoing HTTP calls.
	// +optional
	Inject *OIDCInject `json:"inject,omitempty"`

	// RefreshBefore causes the provider to re-exchange the SA token this long before
	// the cached token's exp claim. Defaults to 5m.
	// +optional
	RefreshBefore *metav1.Duration `json:"refreshBefore,omitempty"`
}

// OIDCExchange configures an RFC 8693 token exchange endpoint.
type OIDCExchange struct {
	// TokenEndpoint is the OIDC STS token exchange URL.
	TokenEndpoint string `json:"tokenEndpoint"`

	// Audience is the intended audience for the exchanged token.
	Audience string `json:"audience"`

	// ExtraParams are provider-specific key-value pairs appended to the exchange request.
	// +optional
	ExtraParams map[string]string `json:"extraParams,omitempty"`
}

// OIDCInject controls how a resolved token is attached to outgoing HTTP calls.
type OIDCInject struct {
	// Type selects the injection strategy. Currently only "header" is supported.
	// +kubebuilder:validation:Enum=header
	// +kubebuilder:default=header
	// +optional
	Type string `json:"type,omitempty"`

	// Header is the HTTP header name used when Type is "header".
	// +optional
	Header string `json:"header,omitempty"`

	// Prefix is prepended to the token value when Type is "header" (e.g. "Bearer ").
	// +optional
	Prefix string `json:"prefix,omitempty"`
}

// MergeOIDCConfigs returns an OIDCConfig where non-zero fields from override
// take precedence over base. Either argument may be nil.
func MergeOIDCConfigs(override, base *OIDCConfig) *OIDCConfig {
	if base == nil && override == nil {
		return nil
	}
	if base == nil {
		out := *override
		return &out
	}
	if override == nil {
		out := *base
		return &out
	}

	merged := *base

	if override.ServiceAccountTokenPath != "" {
		merged.ServiceAccountTokenPath = override.ServiceAccountTokenPath
	}
	merged.Exchange = mergeOIDCExchange(override.Exchange, base.Exchange)
	merged.Inject = mergeOIDCInject(override.Inject, base.Inject)
	if override.RefreshBefore != nil {
		rb := *override.RefreshBefore
		merged.RefreshBefore = &rb
	}

	return &merged
}

// mergeOIDCExchange field-merges override over base so a per-resource override of a single
// field (e.g. audience) does not drop the tokenEndpoint/extraParams inherited from base.
func mergeOIDCExchange(override, base *OIDCExchange) *OIDCExchange {
	if override == nil {
		return base
	}
	if base == nil {
		out := *override
		return &out
	}

	merged := *base
	if override.TokenEndpoint != "" {
		merged.TokenEndpoint = override.TokenEndpoint
	}
	if override.Audience != "" {
		merged.Audience = override.Audience
	}
	// extraParams are merged key-by-key so an override can add or replace individual
	// params while retaining the rest from base.
	if override.ExtraParams != nil {
		if merged.ExtraParams == nil {
			merged.ExtraParams = map[string]string{}
		} else {
			cp := make(map[string]string, len(merged.ExtraParams))
			for k, v := range merged.ExtraParams {
				cp[k] = v
			}
			merged.ExtraParams = cp
		}
		for k, v := range override.ExtraParams {
			merged.ExtraParams[k] = v
		}
	}
	return &merged
}

// mergeOIDCInject field-merges override over base so a partial inject override retains the
// unset fields from base.
func mergeOIDCInject(override, base *OIDCInject) *OIDCInject {
	if override == nil {
		return base
	}
	if base == nil {
		out := *override
		return &out
	}

	merged := *base
	if override.Type != "" {
		merged.Type = override.Type
	}
	if override.Header != "" {
		merged.Header = override.Header
	}
	if override.Prefix != "" {
		merged.Prefix = override.Prefix
	}
	return &merged
}
