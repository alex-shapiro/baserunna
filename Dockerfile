# syntax=docker/dockerfile:1
# Release build of the baserunna server. See DEPLOYING.md.
FROM rust:1.97-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang perl && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY benches benches
COPY examples examples
COPY tests tests
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin baserunna && cp target/release/baserunna /baserunna

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /var/lib/baserunna --create-home baserunna
COPY --from=build /baserunna /usr/local/bin/baserunna
USER baserunna
WORKDIR /var/lib/baserunna
ENV BASERUNNA_LISTEN=0.0.0.0:8080
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/baserunna"]
