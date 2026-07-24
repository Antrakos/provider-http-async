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
	"net/http"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"

	"github.com/Antrakos/provider-http-async/apis/common"
	"github.com/Antrakos/provider-http-async/internal/clients/gcp"
)

// gcpClient decorates an existing Client with native GCP token injection.
type gcpClient struct {
	inner  Client
	source oauth2.TokenSource
}

// gcpHTTPTimeout is the deadline for a single GCP impersonation round trip.
// Long enough for a cold IAM generateAccessToken call across a WAN; short
// enough to surface a hung endpoint before the reconcile context times out.
// (The ADC metadata-server fetch itself is governed by the google SDK.)
const gcpHTTPTimeout = 10 * time.Second

// NewGCPClient wraps inner with native GCP authentication. With no
// serviceAccount the source is Application Default Credentials (the GKE
// metadata server on Workload Identity); with serviceAccount set the ADC
// source impersonates that GCP service account. Tokens are cached and refreshed
// by oauth2.ReuseTokenSource.
func NewGCPClient(ctx context.Context, inner Client, cfg *common.GCPAuth) (Client, error) {
	tokenHTTPClient := &http.Client{Timeout: gcpHTTPTimeout}
	source, err := gcp.New(ctx, cfg, tokenHTTPClient)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build GCP token source")
	}
	return &gcpClient{inner: inner, source: source}, nil
}

func (c *gcpClient) SendRequest(ctx context.Context, method string, url string, body Data, headers Data, tlsConfig *TLSConfigData) (HttpDetails, error) {
	tok, err := c.source.Token()
	if err != nil {
		return HttpDetails{}, errors.Wrap(err, "failed to obtain GCP token")
	}

	// The GCP path injects a standard OAuth2 bearer token into the Authorization
	// header. A caller-set Authorization header always wins (we do not overwrite),
	// matching the OIDC decorator's "only add if absent" convention.
	hdrs, ok := headers.Decrypted.(map[string][]string)
	if !ok {
		hdrs = map[string][]string{}
	}
	if _, exists := hdrs[authKey]; !exists {
		hdrs[authKey] = []string{"Bearer " + tok.AccessToken}
		headers.Decrypted = hdrs

		encHdrs, eok := headers.Encrypted.(map[string][]string)
		if !eok {
			encHdrs = map[string][]string{}
		}
		// Mask the token in the encrypted/log copy so it never lands in status or logs.
		encHdrs[authKey] = []string{"Bearer [GCP-TOKEN]"}
		headers.Encrypted = encHdrs
	}
	return c.inner.SendRequest(ctx, method, url, body, headers, tlsConfig)
}
