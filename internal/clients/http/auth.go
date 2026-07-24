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

package http

import (
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/Antrakos/provider-http-async/apis/common"
)

// AuthSelection captures the effective authentication state of a ProviderConfig
// after merging any per-resource override: which auth mechanisms are set, and
// (for the secret credential source) whether the static credentials block is
// present. It is the input to ValidateAuthSelection.
type AuthSelection struct {
	// CredentialsSet is true when the ProviderConfig carries a credentials block
	// (source is one of None/Secret/InjectedIdentity/Environment/Filesystem).
	CredentialsSet bool
	// CredentialSource is the resolved credentials source, valid only when
	// CredentialsSet is true.
	CredentialSource xpv2.CredentialsSource
	// GCP is the effective GCP auth config, or nil when unset.
	GCP *common.GCPAuth
	// OIDC is the effective OIDC config, or nil when unset.
	OIDC *common.OIDCConfig
}

// Auth-selection validation error sentinels. These are wrapped by the connector
// and surfaced as Synced=False conditions with the matching message.
const (
	errNoAuthMechanism  = "no auth mechanism configured: set one of credentials, gcp, or oidc"
	errGCPAndOIDCMutual = "gcp and oidc are mutually exclusive on a single config"
	errCredAndIdentity  = "credentials cannot be combined with an identity block (gcp or oidc); drop the now-optional credentials block and use the identity block alone"
)

// ValidateAuthSelection enforces the auth-selection rules from the GCP native
// auth PRD against the effective (merged) auth state. It returns nil for a valid
// config, or a descriptive error for:
//
//   - none of credentials/gcp/oidc set (no auth mechanism),
//   - gcp and oidc both set (mutually exclusive),
//   - credentials combined with gcp or oidc (an identity block is the auth
//     mechanism; no static credentials placeholder is needed).
//
// The last rule replaces the pre-PRD silent-override quirk where an identity
// block won and the static token was never sent; configs that relied on it must
// drop the now-optional credentials block.
func ValidateAuthSelection(s AuthSelection) error {
	identitySet := s.GCP != nil || s.OIDC != nil
	if !s.CredentialsSet && !identitySet {
		return &AuthSelectionError{Message: errNoAuthMechanism}
	}
	if s.GCP != nil && s.OIDC != nil {
		return &AuthSelectionError{Message: errGCPAndOIDCMutual}
	}
	if s.CredentialsSet && identitySet {
		return &AuthSelectionError{Message: errCredAndIdentity}
	}
	return nil
}

// AuthSelectionError is returned by ValidateAuthSelection. It carries a stable
// message intended for a Synced=False condition; errors.Is/as against this type
// lets tests assert "a validation error" without matching the exact wording.
type AuthSelectionError struct {
	Message string
}

func (e *AuthSelectionError) Error() string { return e.Message }
