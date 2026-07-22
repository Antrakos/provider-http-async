# provider-http-async

A [Crossplane](https://crossplane.io/) provider for making asynchronous HTTP
requests. It extends the
[provider-http](https://github.com/crossplane-contrib/provider-http) pattern
with async/polling semantics — submit a request, poll for completion, observe
the result as a managed resource.

## Installing

```shell
crossplane xpkg install provider ghcr.io/antrakos/provider-http-async:latest
```

## Local Development

### Prerequisites

- Go 1.25+
- [lefthook](https://github.com/evilmartians/lefthook) for pre-push hooks

### Setup

```shell
# Install git hooks (runs lint + tests before every push)
lefthook install
```

### Common tasks

```shell
# Run tests
go test ./...

# Lint
go tool golangci-lint run ./...

# Regenerate CRDs and deepcopy methods after editing API types
go generate ./apis/...

# Build the provider binary
go build ./cmd/provider

# Run the provider out-of-cluster against your current kubeconfig
go run ./cmd/provider --debug
```

## Releasing

Releases are driven by git tags. Push a `v*` tag to trigger CI to build and
publish the package to GHCR and create a GitHub Release with generated notes:

```shell
git tag v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

Release notes are generated from conventional commits via
[git-cliff](https://git-cliff.org/) using the configuration in `cliff.toml`.
