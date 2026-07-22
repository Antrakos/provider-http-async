# syntax=docker/dockerfile:1

ARG GO_VERSION=1

FROM --platform=${BUILDPLATFORM} golang:${GO_VERSION} AS build

WORKDIR /provider

ENV CGO_ENABLED=0

RUN --mount=target=. --mount=type=cache,target=/go/pkg/mod go mod download

ARG TARGETOS
ARG TARGETARCH

RUN --mount=target=. \
    --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /provider-http-async ./cmd/provider

FROM gcr.io/distroless/static-debian12:nonroot AS image
WORKDIR /
COPY --from=build /provider-http-async /provider-http-async
USER nonroot:nonroot
ENTRYPOINT ["/provider-http-async"]
