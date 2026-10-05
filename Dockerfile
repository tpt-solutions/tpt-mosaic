# Build the node daemon.
FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY benches ./benches
RUN cargo build --release -p tpt-mosaic-node

# Minimal runtime image.
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/tpt-mosaic-node /usr/local/bin/tpt-mosaic-node
# Non-root runtime with a writable home for state files.
RUN useradd --system --create-home --home-dir /var/lib/mosaic mosaic
USER mosaic
WORKDIR /var/lib/mosaic
EXPOSE 7801 7331
ENTRYPOINT ["tpt-mosaic-node"]
CMD ["--config", "/etc/mosaic/node.toml"]
