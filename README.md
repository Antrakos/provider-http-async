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
      url: '"https://us-central1-aiplatform.googleapis.com/v1beta1/" + .response.body.name'
      done: .poll.response.body.done == true
      error: .poll.response.body.error  # non-null → terminal failure (Ready=False, Synced=False)
      timeout: 30m                      # default 30m
      interval: 5s                      # default 5s
```

The reconciler drives the poll loop in the foreground for up to 2 minutes per reconcile
iteration, then requeues. `status.polling.response` is the crash-recovery anchor — the
raw mutate response, persisted before the first poll. A reconcile that finds it set
resumes the in-flight operation (recomputing `polling.url` against it) instead of
re-firing the mutate call, and the anchor is retained across a terminal poll failure so
a corrected `polling.url` resumes the existing operation rather than creating a
duplicate. It is cleared only when the operation completes (or the resource is deleted).

> **`polling.url` must resolve to an absolute URL.** The resolved value is recomputed
> from the mutate response on every reconcile, so it must include a scheme and host
> (`https://host/path`). Many GCP LRO APIs (Vertex AI `models:upload`, `endpoints`,
> `deployModel`, …) return `name` as a **bare resource path** such as
> `projects/123/locations/us-central1/operations/789`, not a full URL. Writing
> `url: .response.body.name` therefore yields a scheme-less string that fails with
> `unsupported protocol scheme ""`. Prepend the base URL as shown above. If the
> expression resolves to a non-absolute (or empty) value the provider stops with a
> **terminal failure** (`Synced=False`) and a descriptive message instead of retrying —
> the anchor is retained, so fixing the expression and bumping the generation resumes
> polling and it does **not** re-fire CREATE, so no duplicate remote resource is minted.

> **Only add `polling` to a mapping whose API returns a long-running operation.** If the
> operation completes synchronously its response carries no operation identifier, so
> `polling.url` resolves to an empty value — a terminal failure with a message steering
> you to remove the `polling` block. Removing it (a spec change) clears the terminal
> state and the resource reconciles synchronously.

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

### Sub-resource existence: `resourceExistsCheck`

Some APIs have no dedicated GET for the resource you manage — the only way to observe
it is to GET a **parent** that always returns `2xx` and check whether your sub-resource
is present in the response. A Vertex AI `deployedModel` is the canonical case: it is
observed via `GET /endpoints/{id}`, which returns `200` whether or not any model is
deployed to the endpoint.

Without a separate existence signal the reconciler treats the parent's `200` as "the
resource exists", so `expectedResponseCheck: false` is read as drift and the controller
calls `Update()` — which then finds no UPDATE mapping and silently skips, never reaching
`Create()`. The resource is stuck.

`resourceExistsCheck` decouples existence from drift. It is a CUSTOM jq expression
evaluated against the OBSERVE response (with `.status.externalRef` available) **after**
the in-flight anchor gate and **before** `expectedResponseCheck`:

- `resourceExistsCheck: false` → the sub-resource is absent → **CREATE**
- `resourceExistsCheck: true` → exists → fall through to `expectedResponseCheck`
  (`false` → UPDATE, `true` → up to date)

> This check is an **override for the 2xx-parent case**, not a replacement for the default
> existence inference. The default — a non-2xx OBSERVE on a first observe (no
> `externalRef`, no prior response) already routes to CREATE, and an `isRemovedCheck` 404
> routes to delete — fires **before** `resourceExistsCheck`, which only runs on a 2xx
> response. That is the one case the HTTP status cannot answer: the parent's `200` tells
> you the parent exists, not whether the sub-resource you own is in it.

> **Identity gate before OBSERVE.** When a resource has never been identified (no
> `externalRef`, no prior response, no in-flight anchor) and its OBSERVE URL is built as
> `baseUrl + "/" + .status.externalRef`, the empty `externalRef` collapses the URL onto the
> resource's *collection* endpoint (e.g. `.../models/`), which a well-behaved API answers
> with `200` + a list body. That `200` is indistinguishable from "the resource exists" using
> the HTTP status alone, so the reconciler routes such a resource to `Create()` **before**
> firing OBSERVE, treating the absence of any established identity as "resource does not
> exist". This applies only when the OBSERVE URL template references `.status.externalRef`
> and no identity has been set; a constant URL (or one keyed by `.response.body.id`) does
> not collapse and OBSERVEs normally, and a resource that already has `externalRef` or a
> prior response OBSERVEs normally too. (The in-flight anchor gate handles its own
> empty-`externalRef` case for the CREATE+poll resume.)

```yaml
mappings:
  - action: CREATE
    method: POST
    url: '.payload.baseUrl + "/endpoints/456:deployModel"'
    body: .payload.body
    polling:
      url: '"https://aiplatform.googleapis.com/v1beta1/" + .response.body.name'
      done: .poll.response.body.done == true
      error: .poll.response.body.error
      timeout: 60m
      interval: 30s
  - action: OBSERVE
    method: GET
    url: '.payload.baseUrl + "/endpoints/456"'   # parent — always 200
  - action: REMOVE
    method: POST
    url: '.payload.baseUrl + "/endpoints/456:undeployModel"'
    body: '{"deployedModelId": .status.externalRef}'
resourceExistsCheck:
  type: CUSTOM
  logic: |
    (.response.body.deployedModels // []) as $m
    | .status.externalRef as $ref
    | ($m | map(select(.id == $ref)) | length > 0)
expectedResponseCheck:
  type: CUSTOM
  logic: |
    (.response.body.deployedModels // []) as $m
    | .status.externalRef as $ref
    | ($m | map(select(.id == $ref)) | length > 0)
```

> Inside `map(select(...))` the `.` is the list element, so `.status` would resolve to
> the element's (null) `.status`, not the root's. Bind the list and `.status.externalRef`
> to variables first, as shown, then reference them inside the `map`.

When unset, or `type: DEFAULT`, existence is inferred from the OBSERVE HTTP status (the
default behavior every existing manifest relies on); `resourceExistsCheck` is then a
no-op and the check is skipped entirely. There is no DEFAULT jq logic because the
default is not a jq expression — it is the HTTP-status inference above. This field is
therefore only meaningful with `type: CUSTOM`.

### Status conditions

The provider owns the `Ready` condition; Crossplane's managed reconciler owns `Synced`.
Failure modes are surfaced so a consumer can tell something is wrong from `kubectl get`
alone, without reading pod logs:

| State | `Ready` | `Synced` | Message |
|---|---|---|---|
| Up to date | `True` (`Available`) | `True` (`ReconcileSuccess`) | — |
| In-flight long-running operation (poll still running, budget exhausted per reconcile) | `False` (`Creating`) | `True` (`ReconcileSuccess`) | — |
| Terminal poll/config failure (bad/empty `polling.url`, `polling.error` non-null, timeout) | `False` (`Unavailable`) | `False` (`ReconcileError`) | `Terminal error: <detail>` |
| CUSTOM check reports drift with no UPDATE/PUT mapping | `False` (`Unavailable`) | `False` (`ReconcileError`) | `Terminal error: no UPDATE or PUT mapping is configured but the resource is out of sync …` |
| Non-polling mutate (CREATE/UPDATE/REMOVE) returns non-2xx not in `allowedStatusCodes` | `True`/`False` (unchanged by reconcile) | `False` (`ReconcileError`) | `HTTP <METHOD> request failed with status code: <code>` |

> The "in-flight" row is the steady state while a long-running operation's poll
> is still running — each reconcile re-enters the poll, exhausts its per-reconcile
> budget, and returns nil, which the reconciler reports as `ReconcileSuccess`
> (it calls `Create()` when `externalRef` is unset, `Update()` when set). The same
> reconcile in which the poll *completes* or *fails terminally* transitions to the
> "Up to date" or "Terminal" row instead, so `Synced` is only reliably `True` while
> the poll keeps running.

> A **non-polling** mutate (a CREATE/UPDATE/REMOVE mapping with no `polling` block) that
> returns a non-2xx status not listed in `allowedStatusCodes` is surfaced as a reconcile
> error (`Synced=False`, `ReconcileError`). The response, status code, and failure counter
> are persisted first, so they stay visible alongside the failed reconcile. Polling
> mutates surface failure through `polling.error` / timeout instead — see the terminal row
> above.

A terminal failure is stable: the provider persists it to
`status.polling.terminalError` (with `observedGeneration`) and stops re-firing
OBSERVE/CREATE/UPDATE — `IsUpToDate` short-circuits on the next reconcile — until the
spec changes; a generation bump clears the terminal and retries. For a poll terminal the
anchor is retained, so a corrected `polling.url` resumes the existing operation rather
than creating a duplicate. The `Terminal error: ` prefix on the `Ready` message lets a
consumer or alert distinguish a stuck state from a transient reconcile error.

> The "drift with no UPDATE mapping" terminal applies to the **CUSTOM**
> `expectedResponseCheck`, which reports drift explicitly (`logic` returns `false`).
> The **DEFAULT** check compares the response to the UPDATE body, so with no UPDATE
> mapping it cannot detect drift and reports up-to-date — the intended behavior for
> create-only resources (CREATE/OBSERVE only) that are in sync, which are not a stuck
> state and stay `Ready=True`. (A DEFAULT-check resource that has drifted but has no
> UPDATE mapping is a pre-existing limitation of the DEFAULT check, not surfaced as a
> terminal — use a CUSTOM `expectedResponseCheck` to get drift detection.)

### Import via annotation

To adopt an existing remote resource without recreating it, annotate the `AsyncRequest`:

```yaml
metadata:
  annotations:
    crossplane.io/external-name: "model-id-789"
```

On the first reconcile the controller seeds `status.externalRef` from this annotation
and immediately observes the resource rather than creating it.

> The annotation value **must differ from `metadata.name`** to be treated as an import.
> Crossplane's default initializer auto-populates `crossplane.io/external-name` with the
> object's own name for every resource, so a value equal to `metadata.name` is ignored
> for seeding (it carries no external identity). Set it to the real remote identifier
> (e.g. `models/model-id-789`) to adopt an existing resource.

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

### GCP native authentication

Configure native Google Cloud auth on a `ProviderConfig`. No projected volume or
OIDC ceremony required — the provider uses the GKE Workload Identity metadata server.

#### IAM prerequisites (one-time admin setup)

1. Bind the provider's KSA to a hub GSA via the standard Workload Identity annotation
   and IAM allow-policy (see the GKE Workload Identity docs).
2. For each spoke GSA named in a `ProviderConfig`, grant the hub
   `roles/iam.serviceAccountTokenCreator`:

   ```hcl
   resource "google_service_account_iam_member" "impersonate_team_a" {
     service_account_id = google_service_account.team_a.name
     role               = "roles/iam.serviceAccountTokenCreator"
     member             = "serviceAccount:hub@my-project.iam.gserviceaccount.com"
   }
   # repeat per target SA
   ```

#### Minimal form — ADC only (pod's bound GSA)

```yaml
apiVersion: http.async.m.crossplane.io/v1alpha2
kind: ProviderConfig
metadata:
  name: vertex-ai-gcp
  namespace: my-team
spec:
  gcp: {}
```

No `credentials` block is needed. The provider calls `google.DefaultTokenSource`,
which on GKE Workload Identity reads the metadata server and returns the pod's
bound GSA token.

#### Hub-and-spoke impersonation (per-ProviderConfig service account)

```yaml
apiVersion: http.async.m.crossplane.io/v1alpha2
kind: ProviderConfig
metadata:
  name: vertex-ai-team-a
  namespace: my-team
spec:
  gcp:
    serviceAccount: team-a@my-project.iam.gserviceaccount.com
    scopes:
      - https://www.googleapis.com/auth/cloud-platform
```

The hub GSA mints a token *as* `team-a` via the IAM `generateAccessToken` endpoint.
Different ProviderConfigs can name different service accounts — one provider pod,
many spoke identities, no service-account keys anywhere.

`gcp` and `oidc` are mutually exclusive on a given config. At least one of
`credentials`, `gcp`, or `oidc` must be set; combining `credentials` with an
identity block is rejected with `Synced=False`.

### jq context

| Expression | `.response` | `.poll.response` | `.status` |
|---|---|---|---|
| `polling.url` | mutate response (stable for whole loop) | — | ✓ |
| `polling.done` / `polling.error` | mutate response | current poll GET response | ✓ |
| `OBSERVE.url` | — | — | ✓ |
| `UPDATE.url` / `DELETE.url` | previous OBSERVE response | — | ✓ |
| `expectedResponseCheck` | current OBSERVE response | — | ✓ |
| `resourceExistsCheck` | current OBSERVE response | — | ✓ |
| `externalRef` | mutate response (sync) | completed operation response (async) | ✓ |

### Differences from provider-http

| Feature | provider-http `Request` | provider-http-async `AsyncRequest` |
|---|---|---|
| Async 202 polling | ✗ | ✓ `polling` block per mapping |
| External ID tracking | ✗ | ✓ `externalRef` + `status.externalRef` |
| Import via annotation | ✗ | ✓ `crossplane.io/external-name` |
| OIDC workload identity | ✗ | ✓ `oidc` on ProviderConfig / resource |
| GCP native auth (ADC + impersonation) | ✗ | ✓ `gcp` on ProviderConfig / resource |
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
