# Build only the keyless PAPER runner. No account credentials are build inputs.
FROM rust:1.89-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --no-default-features --bin paper

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/paper /usr/local/bin/paper

# Railway volumes can replace image-directory ownership with a root-owned
# mount. Use container root for this keyless worker without changing ownership
# of existing volume data. An operator may override the UID after separately
# provisioning writable /data permissions. No privileged container is needed.
USER 0:0
WORKDIR /data

# The binary handles SIGTERM itself; no shell or Cargo sits between it and
# the platform. Configure a persistent volume at /data outside this image.
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/paper"]

# Continuous public-data simulation, not real trading. Each launch starts a
# NEW virtual account and a unique create_new log in /data/paper-runs/; stored
# logs are not automatically resumed. Do not replace this with a fixed output.
CMD ["portfolio", "--symbols", "BTCUSDT,ETHUSDT", "--leverage", "10", "--balance", "100"]
