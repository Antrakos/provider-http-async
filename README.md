# provider-http-async

A [Crossplane](https://crossplane.io/) provider that manages remote resources via HTTP,
with native support for async long-running operations, OIDC workload-identity token
exchange, and stable external-ID tracking. It is a full superset of
[provider-http](https://github.com/crossplane-contrib/provider-http) — every existing
manifest works unchanged; all new features are opt-in.

## Installing

```shell
crossplane xpkg install provider ghcr.io/antrakos/provider-http-async:latest
```

## AsyncRequest

`AsyncRequest` is a Crossplane managed resource that manages a single remote resource
via HTTP. Define mappings for CREATE, OBSERVE, UPDATE, and DELETE — the controller fires
the right one and reconciles toward the desired state.

### Async polling

Add a `polling` block to any mapping to handle 202-accepted long-running operations:

```yaml
mappings:
  - action: CREATE
    method: POST
    url: '.payload.baseUrl + "/models:upload"'
    body: .payload.body
    polling:
      url: .response.body.name          # jq against the mutate response — stable for the full loop
      done: .poll.response.body.done == true
      error: .poll.response.body.error  # non-null → terminal failure (Ready=False, Synced=False)
      timeout: 30m                      # default 30m
      interval: 5s                      # default 5s
```

The reconciler drives the poll loop in the foreground for up to 2 minutes per reconcile
iteration, then requeues. `status.operationRef` is the crash-recovery anchor — a
reconcile that finds it non-empty resumes the in-flight operation instead of re-firing
the mutate call.

### External reference

`externalRef` is a top-level jq expression that extracts the stable identifier of the
remote resource after the operation completes. For async APIs it is evaluated against
`.poll.response`; for sync APIs against `.response`.

```yaml
forProvider:
  externalRef: '.poll.response.body.response.model | split("/") | last'
```

The result is stored in `status.externalRef` and available as `.status.externalRef` in
all subsequent jq expressions — OBSERVE URL, UPDATE URL, DELETE URL, and
`expectedResponseCheck`.

### Import via annotation

To adopt an existing remote resource without recreating it, annotate the `AsyncRequest`:

```yaml
metadata:
  annotations:
    crossplane.io/external-name: "model-id-789"
```

On the first reconcile the controller seeds `status.externalRef` from this annotation
and immediately observes the resource rather than creating it.

### OIDC workload-identity

Configure transparent token exchange on the `ProviderConfig` (provider-wide default) or
directly on an `AsyncRequest` spec (per-resource override). The pod's projected service
account token is exchanged with the configured STS endpoint; no credentials are stored
in etcd.

```yaml
# ProviderConfig
spec:
  oidc:
    exchange:
      tokenEndpoint: https://sts.googleapis.com/v1/token
      audience: //iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/my-pool/providers/my-provider
      extraParams:
        requested_token_type: urn:ietf:params:oauth:token-type:access_token
        scope: https://www.googleapis.com/auth/cloud-platform
    inject:
      type: header        # "header" (default)
      header: Authorization
      prefix: "Bearer "
    refreshBefore: 5m    # re-exchange this long before token expiry
```

Token caching and expiry are handled by `golang.org/x/oauth2.ReuseTokenSource`.

### jq context

| Expression | `.response` | `.poll.response` | `.status` |
|---|---|---|---|
| `polling.url` | mutate response (stable for whole loop) | — | ✓ |
| `polling.done` / `polling.error` | mutate response | current poll GET response | ✓ |
| `OBSERVE.url` | — | — | ✓ |
| `UPDATE.url` / `DELETE.url` | previous OBSERVE response | — | ✓ |
| `expectedResponseCheck` | current OBSERVE response | — | ✓ |
| `externalRef` | mutate response (sync) | completed operation response (async) | ✓ |

### Differences from provider-http

| Feature | provider-http `Request` | provider-http-async `AsyncRequest` |
|---|---|---|
| Async 202 polling | ✗ | ✓ `polling` block per mapping |
| External ID tracking | ✗ | ✓ `externalRef` + `status.externalRef` |
| Import via annotation | ✗ | ✓ `crossplane.io/external-name` |
| OIDC workload identity | ✗ | ✓ `oidc` on ProviderConfig / resource |
| Backward compat | — | ✓ all existing manifests work unchanged |

### Examples

See [`examples/namespaced/`](./examples/namespaced/) and [`examples/cluster/`](./examples/cluster/):

- `vertex-model.yaml` — full Vertex AI async model upload: CREATE+poll, OBSERVE, UPDATE, DELETE+poll
- `vertex-providerconfig.yaml` — OIDC Workload Identity Federation config for GCP
- `vertex-model-import.yaml` — adopting an existing model via `external-name`
- `plain-sync.yaml` — plain synchronous example (no polling, no OIDC)

## Local Development

### Prerequisites

- Go 1.25+
- [lefthook](https://github.com/evilmartians/lefthook) for pre-push hooks

### Setup

```shell
lefthook install
```

### Common tasks

```shell
# Run tests
go test ./...

# Lint
go tool golangci-lint run ./...

# Regenerate deepcopy methods and CRDs after editing API types
go generate ./apis/...

# Build the provider binary
go build ./cmd/provider

# Run the provider out-of-cluster against your current kubeconfig
go run ./cmd/provider --debug
```

## Releasing

Releases are driven by git tags. Push a `v*` tag to trigger CI, build the package, and
create a GitHub Release with generated notes:

```shell
git tag v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

Release notes are generated from conventional commits via
[git-cliff](https://git-cliff.org/) (`cliff.toml`).
