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

package gcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/Antrakos/provider-http-async/apis/common"
)

// stubADC replaces defaultTokenSource with a fixed token and restores it on cleanup.
func stubADC(t *testing.T, tok *oauth2.Token) {
	t.Helper()
	orig := defaultTokenSource
	defaultTokenSource = func(_ context.Context, _ []string) (oauth2.TokenSource, error) {
		return &staticSource{tok: tok}, nil
	}
	t.Cleanup(func() { defaultTokenSource = orig })
}

type staticSource struct{ tok *oauth2.Token }

func (s *staticSource) Token() (*oauth2.Token, error) { return s.tok, nil }

func iamServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func TestNew_ADCOnly_UsesDefaultTokenSource(t *testing.T) {
	want := &oauth2.Token{AccessToken: "adc-token", Expiry: time.Now().Add(time.Hour)}
	stubADC(t, want)

	cfg := &common.GCPAuth{} // no serviceAccount → ADC only
	src, err := New(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	tok, err := src.Token()
	if err != nil {
		t.Fatalf("Token() error: %v", err)
	}
	if tok.AccessToken != "adc-token" {
		t.Errorf("expected adc-token, got %q", tok.AccessToken)
	}
}

func TestNew_ADCOnly_DefaultsScope(t *testing.T) {
	var gotScopes []string
	orig := defaultTokenSource
	defaultTokenSource = func(_ context.Context, scopes []string) (oauth2.TokenSource, error) {
		gotScopes = scopes
		return &staticSource{tok: &oauth2.Token{AccessToken: "t", Expiry: time.Now().Add(time.Hour)}}, nil
	}
	defer func() { defaultTokenSource = orig }()

	_, _ = New(context.Background(), &common.GCPAuth{}, nil)
	if len(gotScopes) != 1 || gotScopes[0] != common.DefaultGCPScope {
		t.Errorf("expected default scope %q, got %v", common.DefaultGCPScope, gotScopes)
	}
}

func TestNew_Impersonation_Success(t *testing.T) {
	stubADC(t, &oauth2.Token{AccessToken: "base-token", Expiry: time.Now().Add(time.Hour)})

	expireTime := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	srv := iamServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer base-token" {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"accessToken": "impersonated-token",
			"expireTime":  expireTime,
		})
	})

	cfg := &common.GCPAuth{ServiceAccount: "sa@project.iam.gserviceaccount.com"}
	imp := &impersonationTokenSource{
		adc:            &staticSource{tok: &oauth2.Token{AccessToken: "base-token", Expiry: time.Now().Add(time.Hour)}},
		serviceAccount: "sa@project.iam.gserviceaccount.com",
		scopes:         []string{common.DefaultGCPScope},
		httpClient:     srv.Client(),
	}
	// Override the endpoint to point at test server.
	tok, err := imp.generateAccessTokenAt(context.Background(), srv.URL+"/v1/projects/-/serviceAccounts/"+cfg.ServiceAccount+":generateAccessToken")
	if err != nil {
		t.Fatalf("generateAccessToken() error: %v", err)
	}
	if tok.AccessToken != "impersonated-token" {
		t.Errorf("expected impersonated-token, got %q", tok.AccessToken)
	}
	if tok.Expiry.IsZero() {
		t.Error("expected non-zero Expiry")
	}
}

func TestNew_Impersonation_HTTPError(t *testing.T) {
	stubADC(t, &oauth2.Token{AccessToken: "base", Expiry: time.Now().Add(time.Hour)})
	srv := iamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"permission denied"}`, http.StatusForbidden)
	})

	imp := &impersonationTokenSource{
		adc:            &staticSource{tok: &oauth2.Token{AccessToken: "base", Expiry: time.Now().Add(time.Hour)}},
		serviceAccount: "sa@project.iam.gserviceaccount.com",
		scopes:         []string{common.DefaultGCPScope},
		httpClient:     srv.Client(),
	}
	_, err := imp.generateAccessTokenAt(context.Background(), srv.URL+"/:generateAccessToken")
	if err == nil {
		t.Fatal("expected error for non-2xx response, got nil")
	}
}

func TestNew_Impersonation_Caches(t *testing.T) {
	stubADC(t, &oauth2.Token{AccessToken: "base", Expiry: time.Now().Add(time.Hour)})

	callCount := 0
	expireTime := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	srv := iamServer(t, func(w http.ResponseWriter, _ *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"accessToken": "cached-impersonated",
			"expireTime":  expireTime,
		})
	})

	cfg := &common.GCPAuth{ServiceAccount: "sa@project.iam.gserviceaccount.com"}
	// Build source with overridden endpoint.
	src, err := newWithEndpoint(context.Background(), cfg, srv.Client(), srv.URL)
	if err != nil {
		t.Fatalf("newWithEndpoint() error: %v", err)
	}
	for i := 0; i < 5; i++ {
		tok, err := src.Token()
		if err != nil {
			t.Fatalf("call %d: Token() error: %v", i, err)
		}
		if tok.AccessToken != "cached-impersonated" {
			t.Errorf("call %d: expected cached-impersonated, got %q", i, tok.AccessToken)
		}
	}
	if callCount != 1 {
		t.Errorf("expected exactly 1 IAM call (ReuseTokenSource should cache), got %d", callCount)
	}
}
