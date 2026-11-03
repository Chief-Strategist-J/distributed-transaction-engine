# syntax=docker/dockerfile:1.7
ARG BUILDER_IMAGE=golang:1.23-bookworm
ARG RUST_BUILDER_IMAGE=rust:1.81-bookworm
ARG CPP_BUILDER_IMAGE=debian:bookworm-slim
ARG BASE_IMAGE=debian:bookworm-slim

# ---------- Stage 1: Rust Module Builder ----------
FROM ${RUST_BUILDER_IMAGE} AS rust-builder
WORKDIR /build/rust
COPY rust-modules/Cargo.toml ./
COPY rust-modules/src/ ./src/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/rust/target \
    cargo build --release && \
    mkdir -p /out/rust && cp target/release/lib* /out/rust/ 2>/dev/null || true

# ---------- Stage 2: C++ Module Builder ----------
FROM ${CPP_BUILDER_IMAGE} AS cpp-builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build/cpp
COPY cpp-modules/ ./
RUN mkdir -p build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release .. && \
    make -j$(nproc) && \
    mkdir -p /out/cpp && cp engine_cpp /out/cpp/ 2>/dev/null || true

# ---------- Stage 3: Go Application Builder ----------
FROM ${BUILDER_IMAGE} AS builder
ARG SERVICE_PATH=.
WORKDIR /build

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64 \
    GOPROXY=https://proxy.golang.org,direct

COPY ${SERVICE_PATH}/go.mod ${SERVICE_PATH}/go.sum* ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY ${SERVICE_PATH}/src/ ./src/
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /build/engine ./src/main.go

# ---------- Stage 4: Minimal Hardened Runtime ----------
FROM ${BASE_IMAGE} AS runtime
ARG SERVICE_PATH=.
ARG UID=10001
ARG GID=10001

RUN groupadd --gid ${GID} app 2>/dev/null || true \
 && useradd --uid ${UID} --gid ${GID} --no-create-home --shell /usr/sbin/nologin app 2>/dev/null || true \
 && mkdir -p /app/config /app/database /app/bin /app/lib && chown -R ${UID}:${GID} /app

WORKDIR /app

COPY --from=builder --chown=${UID}:${GID} /build/engine /app/bin/engine
COPY --from=cpp-builder --chown=${UID}:${GID} /out/cpp/ /app/bin/
COPY --from=rust-builder --chown=${UID}:${GID} /out/rust/ /app/lib/
COPY --chown=${UID}:${GID} ${SERVICE_PATH}/config/ /app/config/
COPY --chown=${UID}:${GID} ${SERVICE_PATH}/database/ /app/database/

ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.source="https://github.com/Chief-Strategist-J/System-Architect" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.title="distributed-transaction-engine"

USER ${UID}:${GID}
EXPOSE 8080 8081
CMD ["/app/bin/engine", "--config", "/app/config/default.yaml"]
