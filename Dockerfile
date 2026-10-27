# syntax=docker/dockerfile:1

# Two ways to build the same image:
#
#   docker build -t pmcd .
#       Compiles pmcd from source (the default, last stage).
#
#   docker buildx build --target prebuilt --platform linux/amd64,linux/arm64 .
#       Packs binaries that CI already built, taken from dist/linux/<arch>/pmcd.
#       Releases are made this way, so no compiler runs under emulation.
#
# Run:
#   docker run -d -p 11211:11211 -p 11212:11212 -v pmcd-data:/data <image>

# The builder's Debian release must match the runtime image (trixie = debian13):
# the binary links against the builder's glibc.
FROM rust:1-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates crates
COPY proto proto
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p pmcd \
 && install -D target/release/pmcd /out/pmcd

# An empty data dir owned by the runtime user: a fresh volume mounted there is then writable.
FROM --platform=$BUILDPLATFORM busybox:1.38 AS dirs
RUN mkdir -p /out/data

# glibc and libgcc only, no shell; runs as uid 65532 (nonroot).
FROM gcr.io/distroless/cc-debian13:nonroot AS runtime
LABEL org.opencontainers.image.title="pmcd" \
      org.opencontainers.image.description="Persistent, replicated, sharded memcached-compatible server" \
      org.opencontainers.image.source="https://github.com/bssth/persistent-memcached" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"
COPY --from=dirs --chown=65532:65532 /out/data /data
VOLUME /data
# memcached text protocol, gRPC (client API and node-to-node traffic)
EXPOSE 11211 11212
ENTRYPOINT ["/usr/local/bin/pmcd"]
CMD ["--data-dir", "/data"]

FROM runtime AS prebuilt
ARG TARGETOS
ARG TARGETARCH
COPY --chmod=755 dist/${TARGETOS}/${TARGETARCH}/pmcd /usr/local/bin/pmcd

FROM runtime AS source
COPY --from=build /out/pmcd /usr/local/bin/pmcd
