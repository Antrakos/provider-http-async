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

func TestMergeGCPConfigs_BothNil(t *testing.T) {
	if got := MergeGCPConfigs(nil, nil); got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestMergeGCPConfigs_BaseNil(t *testing.T) {
	override := &GCPAuth{ServiceAccount: "sa@project.iam.gserviceaccount.com"}
	got := MergeGCPConfigs(override, nil)
	if got == nil || got.ServiceAccount != override.ServiceAccount {
		t.Errorf("expected override returned as-is, got %+v", got)
	}
	if got == override {
		t.Error("expected a copy, got same pointer")
	}
}

func TestMergeGCPConfigs_OverrideNil(t *testing.T) {
	base := &GCPAuth{ServiceAccount: "base@project.iam.gserviceaccount.com"}
	got := MergeGCPConfigs(nil, base)
	if got == nil || got.ServiceAccount != base.ServiceAccount {
		t.Errorf("expected base returned as-is, got %+v", got)
	}
}

func TestMergeGCPConfigs_ServiceAccountOverrideTakesPrecedence(t *testing.T) {
	base := &GCPAuth{ServiceAccount: "base@project.iam.gserviceaccount.com", Scopes: []string{"base-scope"}}
	override := &GCPAuth{ServiceAccount: "override@project.iam.gserviceaccount.com"}
	got := MergeGCPConfigs(override, base)
	if got.ServiceAccount != "override@project.iam.gserviceaccount.com" {
		t.Errorf("expected override serviceAccount, got %q", got.ServiceAccount)
	}
	// override had no scopes — base scopes are kept
	if len(got.Scopes) != 1 || got.Scopes[0] != "base-scope" {
		t.Errorf("expected base scopes when override has none, got %v", got.Scopes)
	}
}

func TestMergeGCPConfigs_ScopesReplace(t *testing.T) {
	base := &GCPAuth{Scopes: []string{"scope-a", "scope-b"}}
	override := &GCPAuth{Scopes: []string{"scope-c"}}
	got := MergeGCPConfigs(override, base)
	if len(got.Scopes) != 1 || got.Scopes[0] != "scope-c" {
		t.Errorf("expected override scopes to replace base, got %v", got.Scopes)
	}
}

func TestMergeGCPConfigs_BaseFillsGaps(t *testing.T) {
	base := &GCPAuth{
		ServiceAccount: "base@project.iam.gserviceaccount.com",
		Scopes:         []string{"https://www.googleapis.com/auth/cloud-platform"},
	}
	override := &GCPAuth{
		// No ServiceAccount and no Scopes — base should fill both in.
	}
	got := MergeGCPConfigs(override, base)
	if got.ServiceAccount != base.ServiceAccount {
		t.Errorf("expected base ServiceAccount, got %q", got.ServiceAccount)
	}
	if len(got.Scopes) != 1 {
		t.Errorf("expected base scopes to be retained, got %v", got.Scopes)
	}
}

func TestMergeGCPConfigs_ScopesCopied(t *testing.T) {
	override := &GCPAuth{Scopes: []string{"scope-x"}}
	got := MergeGCPConfigs(override, nil)
	// Mutating the result must not affect the original.
	got.Scopes[0] = "mutated"
	if override.Scopes[0] != "scope-x" {
		t.Error("MergeGCPConfigs must deep-copy scopes")
	}
}
