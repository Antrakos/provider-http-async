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

// Package gcp implements native Google Cloud authentication for outgoing HTTP
// calls. It exposes a single TokenSource factory backed by Application Default
// Credentials (ADC), optionally impersonating a named GCP service account.
//
// Caching, expiry checks, and refresh are handled by golang.org/x/oauth2, the
// same layer the existing oidc package relies on.
package gcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/Antrakos/provider-http-async/apis/common"
)

// New returns an oauth2.TokenSource backed by Google Application Default
// Credentials. When cfg.ServiceAccount is empty the source is ADC directly —
// on GKE Workload Identity this reads the metadata server and returns the pod's
// bound GSA token. When ServiceAccount is set, the ADC source is used to mint a
// token acting as that GCP service account via the IAM generateAccessToken
// endpoint.
//
// httpClient is used for the impersonation round trip and may be nil
// (http.DefaultClient is the fallback). ADC discovery is performed by the
// google SDK against the metadata server / filesystem and is not governed by
// this client; callers that need a bounded impersonation timeout should pass a
// client with an appropriate Timeout.
func New(ctx context.Context, cfg *common.GCPAuth, httpClient *http.Client) (oauth2.TokenSource, error) {
	if cfg == nil {
		return nil, errors.New("gcp config is nil")
	}

	scopes := cfg.Scopes
	if len(scopes) == 0 {
		scopes = []string{common.DefaultGCPScope}
	}

	adc, err := defaultTokenSource(ctx, scopes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve Google Application Default Credentials")
	}

	if cfg.ServiceAccount == "" {
		// ADC only — wrap in ReuseTokenSource for caching/refresh parity with oidc.
		return oauth2.ReuseTokenSource(nil, adc), nil
	}

	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	imp := &impersonationTokenSource{
		adc:            adc,
		serviceAccount: cfg.ServiceAccount,
		scopes:         scopes,
		httpClient:     httpClient,
	}
	return oauth2.ReuseTokenSource(nil, imp), nil
}

// defaultTokenSource returns the ADC token source for the given scopes. It is a
// seam around google.DefaultTokenSource so the call site is testable without a
// live metadata server — tests swap this var instead of stubbing the network.
var defaultTokenSource = func(ctx context.Context, scopes []string) (oauth2.TokenSource, error) {
	return google.DefaultTokenSource(ctx, scopes...)
}

// impersonationTokenSource implements oauth2.TokenSource by calling the IAM
// Credentials generateAccessToken endpoint to mint an access token as the
// named service account, using the ADC token as the bearer.
//
// This mirrors what google.ImpersonatedTokenSource (internal in
// golang.org/x/oauth2@v0.36.0) and `gcloud ... --impersonate-service-account`
// do: POST the requested scopes to
//
//	https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/<sa>:generateAccessToken
//
// with the base credential's access token in the Authorization header. The
// returned token is the hub-and-spoke pattern — one ADC identity (the pod's
// bound GSA) granted serviceAccountTokenCreator on each spoke GSA.
type impersonationTokenSource struct {
	adc            oauth2.TokenSource
	serviceAccount string
	scopes         []string
	httpClient     *http.Client
}

// Token satisfies oauth2.TokenSource. oauth2.ReuseTokenSource calls this only
// when the cached token is expired or absent; it must not block the caller's
// context, so we derive a background context for the impersonation HTTP call
// and rely on the injected httpClient's timeout for cancellation.
func (s *impersonationTokenSource) Token() (*oauth2.Token, error) {
	return s.generateAccessToken(context.Background())
}

func (s *impersonationTokenSource) generateAccessToken(ctx context.Context) (*oauth2.Token, error) {
	endpoint := fmt.Sprintf(
		"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
		s.serviceAccount,
	)
	return s.generateAccessTokenAt(ctx, endpoint)
}

func (s *impersonationTokenSource) generateAccessTokenAt(ctx context.Context, endpoint string) (*oauth2.Token, error) {
	base, err := s.adc.Token()
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain base ADC token for impersonation")
	}

	// generateAccessToken accepts a list of scopes and, optionally, a delegated
	// chain and a lifetime. We only need scopes here; lifetime defaults to the
	// service-account maximum (1h).
	reqBody := struct {
		Scope []string `json:"scope"`
	}{
		Scope: s.scopes,
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal impersonation request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return nil, errors.Wrap(err, "failed to build impersonation request")
	}
	req.Header.Set("Authorization", "Bearer "+base.AccessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "impersonation request failed")
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read impersonation response")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("impersonation failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"accessToken"`
		ExpireTime  string `json:"expireTime"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return nil, errors.Wrap(err, "failed to parse impersonation response")
	}
	if tokenResp.AccessToken == "" {
		return nil, errors.New("impersonation response missing accessToken")
	}

	tok := &oauth2.Token{AccessToken: tokenResp.AccessToken}
	if tokenResp.ExpireTime != "" {
		// iamcredentials returns an RFC 3339 expireTime; refresh a minute early so
		// we never hand out a token that is already expiring.
		if expiry, err := time.Parse(time.RFC3339, tokenResp.ExpireTime); err == nil {
			tok.Expiry = expiry.Add(-impersonateRefreshSlack)
		}
	}
	if tok.Expiry.IsZero() {
		// Fallback window if the API omits expireTime.
		tok.Expiry = time.Now().Add(55 * time.Minute)
	}
	return tok, nil
}

// impersonateRefreshSlack is how far before the impersonated token's expireTime
// the cached copy is considered stale. The IAM generateAccessToken lifetime is
// capped at 1h, so a 1m slack gives a comfortable re-mint window.
const impersonateRefreshSlack = time.Minute

// newWithEndpoint is a test helper that builds a token source using cfg but
// overrides the IAM generateAccessToken endpoint base URL so tests can point at
// a local httptest.Server without hitting the real IAM API.
func newWithEndpoint(ctx context.Context, cfg *common.GCPAuth, httpClient *http.Client, endpointBase string) (oauth2.TokenSource, error) {
	if cfg == nil {
		return nil, errors.New("gcp config is nil")
	}
	scopes := cfg.Scopes
	if len(scopes) == 0 {
		scopes = []string{common.DefaultGCPScope}
	}
	adc, err := defaultTokenSource(ctx, scopes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve ADC")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	imp := &impersonationTokenSource{
		adc:            adc,
		serviceAccount: cfg.ServiceAccount,
		scopes:         scopes,
		httpClient:     httpClient,
	}
	// Wrap with a custom Token() that calls the overridden endpoint.
	return oauth2.ReuseTokenSource(nil, &endpointOverrideSource{inner: imp, endpointBase: endpointBase}), nil
}

// endpointOverrideSource redirects generateAccessToken calls to a test server.
type endpointOverrideSource struct {
	inner        *impersonationTokenSource
	endpointBase string
}

func (s *endpointOverrideSource) Token() (*oauth2.Token, error) {
	endpoint := fmt.Sprintf(
		"%s/v1/projects/-/serviceAccounts/%s:generateAccessToken",
		s.endpointBase,
		s.inner.serviceAccount,
	)
	return s.inner.generateAccessTokenAt(context.Background(), endpoint)
}
