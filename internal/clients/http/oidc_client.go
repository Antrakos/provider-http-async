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

	"github.com/Antrakos/provider-http-async/apis/common"
	"github.com/Antrakos/provider-http-async/internal/clients/oidc"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

// oidcClient decorates an existing Client with OIDC token injection.
type oidcClient struct {
	inner  Client
	source oauth2.TokenSource
	inject *common.OIDCInject
}

// oidcHTTPTimeout is the deadline for a single OIDC token-exchange round trip.
// Long enough for a cold STS call across a WAN; short enough to surface hung
// endpoints before the reconcile context times out.
const oidcHTTPTimeout = 10 * time.Second

// NewOIDCClient wraps inner with OIDC token exchange using the given config.
// Tokens are cached and refreshed by oauth2.ReuseTokenSource.
func NewOIDCClient(inner Client, cfg *common.OIDCConfig) Client {
	tokenHTTPClient := &http.Client{Timeout: oidcHTTPTimeout}
	return &oidcClient{
		inner:  inner,
		source: oidc.New(cfg, tokenHTTPClient),
		inject: cfg.Inject,
	}
}

func (c *oidcClient) SendRequest(ctx context.Context, method string, url string, body Data, headers Data, tlsConfig *TLSConfigData) (HttpDetails, error) {
	tok, err := c.source.Token()
	if err != nil {
		return HttpDetails{}, errors.Wrap(err, "failed to obtain OIDC token")
	}

	injectType := ""
	if c.inject != nil {
		injectType = c.inject.Type
	}

	switch injectType {
	default: // "header" or empty — Bearer header injection
		headerName := "Authorization"
		prefix := "Bearer "
		if c.inject != nil {
			if c.inject.Header != "" {
				headerName = c.inject.Header
			}
			// An explicit empty Prefix means "no prefix"; only default when inject is nil.
			prefix = c.inject.Prefix
		}

		hdrs, ok := headers.Decrypted.(map[string][]string)
		if !ok {
			hdrs = map[string][]string{}
		}
		if _, exists := hdrs[headerName]; !exists {
			hdrs[headerName] = []string{prefix + tok.AccessToken}
			headers.Decrypted = hdrs

			encHdrs, eok := headers.Encrypted.(map[string][]string)
			if !eok {
				encHdrs = map[string][]string{}
			}
			encHdrs[headerName] = []string{prefix + "[OIDC-TOKEN]"}
			headers.Encrypted = encHdrs
		}
		return c.inner.SendRequest(ctx, method, url, body, headers, tlsConfig)
	}
}
