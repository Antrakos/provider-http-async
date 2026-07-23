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

package oidc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/Antrakos/provider-http-async/apis/common"
)

func writeSAToken(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "token")
	if err := os.WriteFile(p, []byte(content), 0600); err != nil {
		t.Fatalf("failed to write SA token: %v", err)
	}
	return p
}

func tokenEndpoint(t *testing.T, resp interface{}, status int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	}))
}

func TestNew_ExchangeSuccess(t *testing.T) {
	saPath := writeSAToken(t, "fake-sa-token")
	srv := tokenEndpoint(t, map[string]interface{}{
		"access_token": "exchanged-token-123",
		"expires_in":   3600,
	}, http.StatusOK)
	defer srv.Close()

	cfg := &common.OIDCConfig{
		ServiceAccountTokenPath: saPath,
		Exchange: &common.OIDCExchange{
			TokenEndpoint: srv.URL,
			Audience:      "my-audience",
		},
	}
	src := New(cfg, nil)
	tok, err := src.Token()
	if err != nil {
		t.Fatalf("Token() unexpected error: %v", err)
	}
	if tok.AccessToken != "exchanged-token-123" {
		t.Errorf("expected access_token=exchanged-token-123, got %q", tok.AccessToken)
	}
	if tok.Expiry.IsZero() {
		t.Error("expected non-zero Expiry")
	}
}

func TestNew_Caches_Token(t *testing.T) {
	saPath := writeSAToken(t, "sa-token")
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"access_token": "cached-token",
			"expires_in":   3600,
		})
	}))
	defer srv.Close()

	cfg := &common.OIDCConfig{
		ServiceAccountTokenPath: saPath,
		Exchange: &common.OIDCExchange{
			TokenEndpoint: srv.URL,
			Audience:      "aud",
		},
	}
	src := New(cfg, nil)

	for i := 0; i < 5; i++ {
		tok, err := src.Token()
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", i, err)
		}
		if tok.AccessToken != "cached-token" {
			t.Errorf("call %d: expected cached-token, got %q", i, tok.AccessToken)
		}
	}
	if callCount != 1 {
		t.Errorf("expected exactly 1 exchange call, got %d (ReuseTokenSource should cache)", callCount)
	}
}

func TestNew_ExchangeHTTPError(t *testing.T) {
	saPath := writeSAToken(t, "sa-token")
	srv := tokenEndpoint(t, map[string]interface{}{"error": "unauthorized"}, http.StatusUnauthorized)
	defer srv.Close()

	cfg := &common.OIDCConfig{
		ServiceAccountTokenPath: saPath,
		Exchange: &common.OIDCExchange{
			TokenEndpoint: srv.URL,
			Audience:      "aud",
		},
	}
	_, err := New(cfg, nil).Token()
	if err == nil {
		t.Fatal("expected error for non-2xx response, got nil")
	}
}

func TestNew_MissingAccessToken(t *testing.T) {
	saPath := writeSAToken(t, "sa-token")
	srv := tokenEndpoint(t, map[string]interface{}{"other_field": "value"}, http.StatusOK)
	defer srv.Close()

	cfg := &common.OIDCConfig{
		ServiceAccountTokenPath: saPath,
		Exchange: &common.OIDCExchange{
			TokenEndpoint: srv.URL,
			Audience:      "aud",
		},
	}
	_, err := New(cfg, nil).Token()
	if err == nil {
		t.Fatal("expected error for missing access_token, got nil")
	}
}

func TestNew_RefreshBefore_FallbackExpiry(t *testing.T) {
	saPath := writeSAToken(t, "sa-token")
	srv := tokenEndpoint(t, map[string]interface{}{
		// No expires_in — should fall back to refreshBefore window
		"access_token": "short-lived",
	}, http.StatusOK)
	defer srv.Close()

	rb := 10 * time.Minute
	cfg := &common.OIDCConfig{
		ServiceAccountTokenPath: saPath,
		Exchange: &common.OIDCExchange{
			TokenEndpoint: srv.URL,
			Audience:      "aud",
		},
		RefreshBefore: &metav1.Duration{Duration: rb},
	}
	tok, err := New(cfg, nil).Token()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Expiry should be ~refreshBefore from now (used as the fallback window)
	wantAfter := time.Now().Add(rb / 2)
	if tok.Expiry.Before(wantAfter) {
		t.Errorf("expiry %v is too soon; expected at least %v", tok.Expiry, wantAfter)
	}
}
