# syntax=docker/dockerfile:1

# Cross-compilation helper toolkit
FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.6.1 AS xx

# Build stage on host architecture
FROM --platform=$BUILDPLATFORM rust:alpine AS builder

# Copy xx cross-compilation helpers
COPY --from=xx / /

# Install host build tools
RUN apk add --no-cache \
    clang \
    lld \
    musl-dev \
    pkgconfig \
    make \
    git \
    protobuf-dev

ARG TARGETPLATFORM

# Install target sysroot and C/C++ cross-compilers for SQLite bundled build
RUN xx-apk add --no-cache \
    musl-dev \
    gcc \
    g++

# Configure Rust target triple for TARGETPLATFORM
RUN xx-cargo --setup-target-triple

WORKDIR /usr/src/rsqlite-rsync

# Copy manifest files and all source trees referenced by Cargo.toml
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
COPY benches ./benches
COPY tests ./tests

# Build release binary for target architecture and verify ELF architecture
RUN xx-cargo build --release --locked && \
    xx-verify ./target/$(xx-cargo --print-target-triple)/release/rsqlite-rsync && \
    cp ./target/$(xx-cargo --print-target-triple)/release/rsqlite-rsync /usr/local/bin/rsqlite-rsync

# Final runtime image
FROM alpine:3.21 AS runtime

RUN apk add --no-cache \
    ca-certificates \
    kubectl \
    openssh-client \
    sqlite \
    tzdata

# Setup non-root service user and directory structure
RUN addgroup -S -g 10001 rsqlite && \
    adduser -S -u 10001 -G rsqlite -h /var/lib/sqlite rsqlite && \
    mkdir -p /var/lib/sqlite /var/run/rsqlite-rsync /var/log/rsqlite-rsync && \
    chown -R rsqlite:rsqlite /var/lib/sqlite /var/run/rsqlite-rsync /var/log/rsqlite-rsync

COPY --from=builder /usr/local/bin/rsqlite-rsync /usr/local/bin/rsqlite-rsync

USER rsqlite:rsqlite
WORKDIR /var/lib/sqlite

ENTRYPOINT ["/usr/local/bin/rsqlite-rsync"]
CMD ["--help"]
