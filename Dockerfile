# syntax=docker/dockerfile:1
#
# Build:  docker build -t telemetry-pipeline .
# Run:    docker run --rm -v $PWD/pipeline.yaml:/etc/pipeline/pipeline.yaml \
#           -p 9180:9180 -p 9273:9273 -p 57500:57500 telemetry-pipeline

FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/pipeline ./cmd/pipeline

# Static, shell-less, non-root runtime image (~20 MB).
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/pipeline /usr/local/bin/pipeline
COPY examples/minimal.yaml /etc/pipeline/pipeline.yaml
USER nonroot:nonroot
# 9180 admin/self-metrics, 9273 Prometheus exporter, 57500 MDT gRPC dial-out
EXPOSE 9180 9273 57500
ENTRYPOINT ["/usr/local/bin/pipeline"]
CMD ["-config", "/etc/pipeline/pipeline.yaml"]
