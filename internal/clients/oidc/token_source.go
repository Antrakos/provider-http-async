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

// Package oidc implements RFC 8693 token exchange for workload identity.
// Caching, expiry, and thread-safety are handled by golang.org/x/oauth2.
package oidc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"

	"github.com/Antrakos/provider-http-async/apis/common"
)

// New returns an oauth2.TokenSource that performs RFC 8693 token exchange using
// the pod's projected service account token as the subject_token.
// Caching, expiry checks, and refresh are handled by oauth2.ReuseTokenSource.
// httpClient may be nil; http.DefaultClient is used only as a last resort — callers
// should pass a client with an appropriate timeout and TLS configuration.
func New(cfg *common.OIDCConfig, httpClient *http.Client) oauth2.TokenSource {
	refreshBefore := common.ParseDuration(cfg.RefreshBefore, common.DefaultOIDCRefreshBefore)
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	inner := &exchangeTokenSource{cfg: cfg, refreshBefore: refreshBefore, httpClient: httpClient}
	return oauth2.ReuseTokenSource(nil, inner)
}

// exchangeTokenSource implements oauth2.TokenSource by posting an RFC 8693
// subject_token exchange to the configured STS endpoint.
// It is wrapped by oauth2.ReuseTokenSource which handles caching and refresh.
type exchangeTokenSource struct {
	cfg           *common.OIDCConfig
	refreshBefore time.Duration
	httpClient    *http.Client
}

// Token satisfies oauth2.TokenSource. oauth2.ReuseTokenSource calls this when
// the cached token is expired or absent; it must not block the caller's context,
// so we derive a background context for the exchange HTTP call and rely on the
// injected httpClient's timeout for cancellation.
func (s *exchangeTokenSource) Token() (*oauth2.Token, error) {
	return s.exchange(context.Background())
}

func (s *exchangeTokenSource) exchange(ctx context.Context) (*oauth2.Token, error) {
	tokenPath := s.cfg.ServiceAccountTokenPath
	if tokenPath == "" {
		tokenPath = common.DefaultServiceAccountTokenPath
	}

	saToken, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read service account token")
	}

	ex := s.cfg.Exchange
	if ex == nil {
		return nil, errors.New("oidc.exchange is required")
	}

	formData := url.Values{}
	formData.Set("audience", ex.Audience)
	formData.Set("subject_token", string(saToken))
	if _, hasType := ex.ExtraParams["subject_token_type"]; !hasType {
		formData.Set("subject_token_type", "urn:ietf:params:oauth:token-type:jwt")
	}
	if _, has := ex.ExtraParams["grant_type"]; !has {
		formData.Set("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange")
	}
	for k, v := range ex.ExtraParams {
		formData.Set(k, v)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ex.TokenEndpoint,
		strings.NewReader(formData.Encode()))
	if err != nil {
		return nil, errors.Wrap(err, "failed to build token exchange request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "token exchange request failed")
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read token exchange response")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("token exchange failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, errors.Wrap(err, "failed to parse token exchange response")
	}
	if tokenResp.AccessToken == "" {
		return nil, errors.New("token exchange response missing access_token")
	}

	tok := &oauth2.Token{
		AccessToken: tokenResp.AccessToken,
	}
	// Prefer expires_in (authoritative per GCP STS); fall back to the JWT exp claim;
	// last resort is refreshBefore so we always re-exchange before expiry is unknown.
	if tokenResp.ExpiresIn > 0 {
		tok.Expiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn)*time.Second - s.refreshBefore)
	} else if exp, err := parseJWTExp(tokenResp.AccessToken); err == nil {
		tok.Expiry = time.Unix(exp, 0).Add(-s.refreshBefore)
	} else {
		tok.Expiry = time.Now().Add(s.refreshBefore)
	}
	return tok, nil
}

// parseJWTExp decodes the middle (payload) segment of a JWT and returns the exp claim.
// It does not verify the signature; expiry is advisory for refresh scheduling only.
func parseJWTExp(token string) (int64, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0, fmt.Errorf("not a JWT: expected 3 parts, got %d", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0, fmt.Errorf("failed to decode JWT payload: %w", err)
	}
	var claims struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return 0, fmt.Errorf("failed to unmarshal JWT payload: %w", err)
	}
	if claims.Exp == 0 {
		return 0, fmt.Errorf("JWT exp claim is zero or absent")
	}
	return claims.Exp, nil
}
