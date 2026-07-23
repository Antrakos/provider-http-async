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

Token caching and expiry are handled by `golang.org/x/oauth2.ReuseTokenSource`.

#### GKE Workload Identity (full setup)

GKE automatically provisions a Workload Identity Pool per project (`<project-id>.svc.id.goog`).
No custom pool is needed, and no GCP Service Account is required — grant IAM roles directly
to the provider's federated KSA identity.

The STS exchange produces a **federated identity token** representing the Kubernetes SA
(`principal://iam.googleapis.com/.../subject/system:serviceaccount:crossplane-system:provider-http-async`).
This token is sufficient to call GCP APIs — you just bind the required role to that principal
directly instead of going through a GCP SA.

**1. IAM permissions (Terraform)**

```hcl
resource "google_project_iam_member" "provider_http_async_vertex" {
  project = var.project
  role    = "roles/aiplatform.user"   # or whichever role the API requires
  member  = "principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/system:serviceaccount:crossplane-system:provider-http-async"
}
```

`<PROJECT_NUMBER>` is the numeric project ID; `<PROJECT_ID>` is the string project ID —
both from the GKE cluster's host project.

**2. Pin the provider's Kubernetes Service Account name**

Use a `DeploymentRuntimeConfig` to give the provider a stable, predictable SA name.
This name is what you reference in the IAM binding member above.

```yaml
apiVersion: pkg.crossplane.io/v1beta1
kind: DeploymentRuntimeConfig
metadata:
  name: provider-http-async
spec:
  serviceAccountTemplate:
    metadata:
      name: provider-http-async   # stable name; matches the IAM binding member
  deploymentTemplate:
    spec:
      selector: {}
      template:
        spec:
          volumes:
            - name: wi-token-gcp
              projected:
                sources:
                  - serviceAccountToken:
                      # Must match oidc.exchange.audience in the ProviderConfig
                      audience: "//iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/providers/kubernetes"
                      expirationSeconds: 3600
                      path: token
          containers:
            - name: package-runtime
              volumeMounts:
                - name: wi-token-gcp
                  # Custom path avoids collision with the default SA token mount
                  mountPath: /var/run/secrets/oidc/gcp
                  readOnly: true
```

The provider cannot use the GKE metadata server shortcut because it needs the token
explicitly to inject into outgoing HTTP headers. The projected volumes supply those tokens.

**3. ProviderConfig**

```yaml
apiVersion: http.async.m.crossplane.io/v1alpha2
kind: ProviderConfig
metadata:
  name: vertex-ai-gcp
  namespace: my-team
spec:
  credentials:
    source: None    # OIDC handles auth; no secret needed
  oidc:
    # Path must match the mountPath in the provider Deployment
    serviceAccountTokenPath: /var/run/secrets/oidc/gcp/token
    exchange:
      tokenEndpoint: https://sts.googleapis.com/v1/token
      audience: "//iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/providers/kubernetes"
      extraParams:
        grant_type: urn:ietf:params:oauth:grant-type:token-exchange
        requested_token_type: urn:ietf:params:oauth:token-type:access_token
        scope: https://www.googleapis.com/auth/cloud-platform
    inject:
      type: header
      header: Authorization
      prefix: "Bearer "
    refreshBefore: 5m
```

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
