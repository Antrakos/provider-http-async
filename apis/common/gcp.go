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

// DefaultGCPScope is the OAuth scope requested when a GCP config does not specify
// its own scopes. cloud-platform is the broad single scope that authorizes every
// Google Cloud API; it keeps the credential-free GCP path a "just works" default.
// Users who want least-privilege can set an explicit scope on the config.
const DefaultGCPScope = "https://www.googleapis.com/auth/cloud-platform"

// GCPAuth configures native Google Cloud authentication for outgoing HTTP calls.
//
// It is the credential-free, identity-based GCP path: when no serviceAccount is
// set the provider uses Google Application Default Credentials (ADC) — the same
// discovery chain every GCP SDK and gcloud use (GKE metadata server → instance
// metadata → key file). When serviceAccount is set, the ADC source is used to
// impersonate the named GCP service account, allowing different ProviderConfigs
// (and, optionally, per-resource overrides) to act as different GCP service
// accounts from a single provider pod. Tokens are resolved at call time and
// cached in memory; no credentials are stored in etcd.
//
// GCP and oidc are mutually exclusive on a given config: gcp targets the GKE
// metadata-server / ADC chain, while oidc targets a raw RFC 8693 token exchange
// (useful off-platform or for per-SA federation). See docs/prd-gcp-native-auth.md.
type GCPAuth struct {
	// ServiceAccount is the fully-qualified GCP service-account email to impersonate.
	// When omitted the provider uses Application Default Credentials directly
	// (on GKE Workload Identity this is the pod's bound GSA, with no projected
	// volume and no per-config ceremony beyond enabling gcp).
	// +optional
	ServiceAccount string `json:"serviceAccount,omitempty"`

	// Scopes are the OAuth scopes requested for the resulting access token. When
	// omitted the provider defaults to https://www.googleapis.com/auth/cloud-platform.
	// Override with a narrower scope for least-privilege.
	// +optional
	Scopes []string `json:"scopes,omitempty"`
}

// MergeGCPConfigs returns a GCPAuth where non-zero fields from override take
// precedence over base. Either argument may be nil.
//
// Field-merge convention matches MergeOIDCConfigs: a non-empty override value
// wins, and Scopes from override replace (not append to) base scopes. This lets
// a per-resource override name a different serviceAccount without dropping the
// scopes inherited from the ProviderConfig.
func deepCopyGCP(in *GCPAuth) *GCPAuth {
	out := *in
	if in.Scopes != nil {
		out.Scopes = append([]string(nil), in.Scopes...)
	}
	return &out
}

func MergeGCPConfigs(override, base *GCPAuth) *GCPAuth {
	if base == nil && override == nil {
		return nil
	}
	if base == nil {
		return deepCopyGCP(override)
	}
	if override == nil {
		return deepCopyGCP(base)
	}

	merged := *base
	if override.ServiceAccount != "" {
		merged.ServiceAccount = override.ServiceAccount
	}
	if len(override.Scopes) > 0 {
		// Scopes replace, mirroring the "slices replace" convention of the OIDC merge.
		merged.Scopes = append([]string(nil), override.Scopes...)
	}
	return &merged
}
