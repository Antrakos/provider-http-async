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
	"testing"
)

func TestMergeOIDCConfigs_BothNil(t *testing.T) {
	if got := MergeOIDCConfigs(nil, nil); got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestMergeOIDCConfigs_BaseNil(t *testing.T) {
	override := &OIDCConfig{ServiceAccountTokenPath: "/override"}
	got := MergeOIDCConfigs(override, nil)
	if got == nil || got.ServiceAccountTokenPath != "/override" {
		t.Errorf("expected override returned as-is, got %+v", got)
	}
	// Must be a copy, not the same pointer
	if got == override {
		t.Error("expected a copy, got same pointer")
	}
}

func TestMergeOIDCConfigs_OverrideNil(t *testing.T) {
	base := &OIDCConfig{ServiceAccountTokenPath: "/base"}
	got := MergeOIDCConfigs(nil, base)
	if got == nil || got.ServiceAccountTokenPath != "/base" {
		t.Errorf("expected base returned as-is, got %+v", got)
	}
}

func TestMergeOIDCConfigs_OverrideTakesPrecedence(t *testing.T) {
	base := &OIDCConfig{
		ServiceAccountTokenPath: "/base",
		Exchange:                &OIDCExchange{TokenEndpoint: "base-endpoint", Audience: "base-aud"},
	}
	override := &OIDCConfig{
		ServiceAccountTokenPath: "/override",
		Exchange:                &OIDCExchange{TokenEndpoint: "override-endpoint", Audience: "override-aud"},
	}
	got := MergeOIDCConfigs(override, base)
	if got.ServiceAccountTokenPath != "/override" {
		t.Errorf("expected /override, got %q", got.ServiceAccountTokenPath)
	}
	if got.Exchange.TokenEndpoint != "override-endpoint" {
		t.Errorf("expected override-endpoint, got %q", got.Exchange.TokenEndpoint)
	}
}

func TestMergeOIDCConfigs_BaseFillsGaps(t *testing.T) {
	base := &OIDCConfig{
		ServiceAccountTokenPath: "/base",
		Inject:                  &OIDCInject{Header: "X-Token"},
	}
	override := &OIDCConfig{
		// No ServiceAccountTokenPath, no Inject — base should fill in
		Exchange: &OIDCExchange{TokenEndpoint: "ep", Audience: "aud"},
	}
	got := MergeOIDCConfigs(override, base)
	if got.ServiceAccountTokenPath != "/base" {
		t.Errorf("expected base ServiceAccountTokenPath, got %q", got.ServiceAccountTokenPath)
	}
	if got.Inject == nil || got.Inject.Header != "X-Token" {
		t.Errorf("expected base Inject, got %+v", got.Inject)
	}
	if got.Exchange == nil || got.Exchange.TokenEndpoint != "ep" {
		t.Errorf("expected override Exchange, got %+v", got.Exchange)
	}
}
