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

	"github.com/Antrakos/provider-http-async/apis/common"
)

// staticTokenSource is a test-only oauth2.TokenSource that returns a fixed token.
type staticTokenSource struct {
	tok *oauth2.Token
}

func (s *staticTokenSource) Token() (*oauth2.Token, error) {
	return s.tok, nil
}

// captureClient records the headers it received.
type captureClient struct {
	lastHeaders Data
}

func (c *captureClient) SendRequest(_ context.Context, _ string, _ string, _ Data, headers Data, _ *TLSConfigData) (HttpDetails, error) {
	c.lastHeaders = headers
	return HttpDetails{}, nil
}

func newOIDCClientWithSource(inner Client, source oauth2.TokenSource, inject *common.OIDCInject) Client {
	return &oidcClient{inner: inner, source: source, inject: inject}
}

func TestOIDCClient_HeaderInjection(t *testing.T) {
	inner := &captureClient{}
	src := &staticTokenSource{tok: &oauth2.Token{
		AccessToken: "my-token",
		Expiry:      time.Now().Add(time.Hour),
	}}

	c := newOIDCClientWithSource(inner, src, &common.OIDCInject{
		Type:   common.InjectTypeHeader,
		Header: "Authorization",
		Prefix: "Bearer ",
	})

	_, err := c.SendRequest(context.Background(), "GET", "http://example.com",
		Data{Encrypted: "", Decrypted: ""},
		Data{Encrypted: map[string][]string{}, Decrypted: map[string][]string{}},
		nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	hdrs := inner.lastHeaders.Decrypted.(map[string][]string)
	got := hdrs["Authorization"]
	if len(got) == 0 || got[0] != "Bearer my-token" {
		t.Errorf("expected Authorization: Bearer my-token, got %v", got)
	}

	encHdrs := inner.lastHeaders.Encrypted.(map[string][]string)
	encGot := encHdrs["Authorization"]
	if len(encGot) == 0 || encGot[0] != "Bearer [OIDC-TOKEN]" {
		t.Errorf("expected encrypted header to be masked, got %v", encGot)
	}
}

func TestOIDCClient_DoesNotOverwriteExistingHeader(t *testing.T) {
	inner := &captureClient{}
	src := &staticTokenSource{tok: &oauth2.Token{AccessToken: "injected", Expiry: time.Now().Add(time.Hour)}}

	c := newOIDCClientWithSource(inner, src, nil)

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

func TestOIDCClient_CustomHeaderName(t *testing.T) {
	inner := &captureClient{}
	src := &staticTokenSource{tok: &oauth2.Token{AccessToken: "tok", Expiry: time.Now().Add(time.Hour)}}

	c := newOIDCClientWithSource(inner, src, &common.OIDCInject{
		Header: "X-Auth-Token",
		Prefix: "",
	})

	_, _ = c.SendRequest(context.Background(), "GET", "http://example.com",
		Data{Encrypted: "", Decrypted: ""},
		Data{Encrypted: map[string][]string{}, Decrypted: map[string][]string{}},
		nil)

	hdrs := inner.lastHeaders.Decrypted.(map[string][]string)
	if hdrs["X-Auth-Token"][0] != "tok" {
		t.Errorf("expected X-Auth-Token: tok, got %v", hdrs["X-Auth-Token"])
	}
}
