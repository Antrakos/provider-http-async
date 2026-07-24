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
	"context"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// newGCPClientWithSource builds a gcpClient directly from a pre-built TokenSource,
// bypassing ADC resolution, so tests don't need a live metadata server.
func newGCPClientWithSource(inner Client, source oauth2.TokenSource) Client {
	return &gcpClient{inner: inner, source: source}
}

func TestGCPClient_HeaderInjection(t *testing.T) {
	inner := &captureClient{}
	src := &staticTokenSource{tok: &oauth2.Token{
		AccessToken: "gcp-access-token",
		Expiry:      time.Now().Add(time.Hour),
	}}

	c := newGCPClientWithSource(inner, src)
	_, err := c.SendRequest(context.Background(), "GET", "http://example.com",
		Data{Encrypted: "", Decrypted: ""},
		Data{Encrypted: map[string][]string{}, Decrypted: map[string][]string{}},
		nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hdrs := inner.lastHeaders.Decrypted.(map[string][]string)
	got := hdrs["Authorization"]
	if len(got) == 0 || got[0] != "Bearer gcp-access-token" {
		t.Errorf("expected Authorization: Bearer gcp-access-token, got %v", got)
	}
}

func TestGCPClient_MasksTokenInEncryptedCopy(t *testing.T) {
	inner := &captureClient{}
	src := &staticTokenSource{tok: &oauth2.Token{
		AccessToken: "secret-gcp-token",
		Expiry:      time.Now().Add(time.Hour),
	}}

	c := newGCPClientWithSource(inner, src)
	_, _ = c.SendRequest(context.Background(), "GET", "http://example.com",
		Data{Encrypted: "", Decrypted: ""},
		Data{Encrypted: map[string][]string{}, Decrypted: map[string][]string{}},
		nil)

	encHdrs := inner.lastHeaders.Encrypted.(map[string][]string)
	encGot := encHdrs["Authorization"]
	if len(encGot) == 0 || encGot[0] != "Bearer [GCP-TOKEN]" {
		t.Errorf("expected encrypted header to be masked as [GCP-TOKEN], got %v", encGot)
	}
}

func TestGCPClient_DoesNotOverwriteExistingHeader(t *testing.T) {
	inner := &captureClient{}
	src := &staticTokenSource{tok: &oauth2.Token{AccessToken: "gcp-token", Expiry: time.Now().Add(time.Hour)}}

	c := newGCPClientWithSource(inner, src)
	preSet := map[string][]string{"Authorization": {"pre-set"}}
	_, err := c.SendRequest(context.Background(), "GET", "http://example.com",
		Data{Encrypted: "", Decrypted: ""},
		Data{Encrypted: preSet, Decrypted: preSet},
		nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hdrs := inner.lastHeaders.Decrypted.(map[string][]string)
	if hdrs["Authorization"][0] != "pre-set" {
		t.Errorf("expected pre-set header to be preserved, got %v", hdrs["Authorization"])
	}
}
