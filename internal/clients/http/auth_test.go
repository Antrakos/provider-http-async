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
	"errors"
	"testing"

	"github.com/Antrakos/provider-http-async/apis/common"
)

func TestValidateAuthSelection(t *testing.T) {
	gcp := &common.GCPAuth{}
	oidc := &common.OIDCConfig{}

	cases := []struct {
		name    string
		sel     AuthSelection
		wantErr bool
		wantMsg string
	}{
		{
			name:    "credentials only is valid",
			sel:     AuthSelection{CredentialsSet: true},
			wantErr: false,
		},
		{
			name:    "gcp only is valid",
			sel:     AuthSelection{GCP: gcp},
			wantErr: false,
		},
		{
			name:    "oidc only is valid",
			sel:     AuthSelection{OIDC: oidc},
			wantErr: false,
		},
		{
			name:    "none set — no auth mechanism",
			sel:     AuthSelection{},
			wantErr: true,
			wantMsg: errNoAuthMechanism,
		},
		{
			name:    "gcp and oidc both set — mutually exclusive",
			sel:     AuthSelection{GCP: gcp, OIDC: oidc},
			wantErr: true,
			wantMsg: errGCPAndOIDCMutual,
		},
		{
			name:    "credentials + gcp — rejected",
			sel:     AuthSelection{CredentialsSet: true, GCP: gcp},
			wantErr: true,
			wantMsg: errCredAndIdentity,
		},
		{
			name:    "credentials + oidc — rejected",
			sel:     AuthSelection{CredentialsSet: true, OIDC: oidc},
			wantErr: true,
			wantMsg: errCredAndIdentity,
		},
		{
			name:    "credentials + gcp + oidc — rejected (mutual exclusive takes priority)",
			sel:     AuthSelection{CredentialsSet: true, GCP: gcp, OIDC: oidc},
			wantErr: true,
			wantMsg: errGCPAndOIDCMutual,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateAuthSelection(tc.sel)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				var authErr *AuthSelectionError
				if !errors.As(err, &authErr) {
					t.Errorf("expected *AuthSelectionError, got %T: %v", err, err)
				}
				if tc.wantMsg != "" && authErr.Message != tc.wantMsg {
					t.Errorf("expected message %q, got %q", tc.wantMsg, authErr.Message)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
