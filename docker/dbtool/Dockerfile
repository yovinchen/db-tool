ARG RUST_IMAGE=rust:1-bookworm@sha256:77fac8b98f9f46062bb680b6d25d5bcaabfc400143952ebc572e924bcbedc3fa
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

FROM ${RUST_IMAGE} AS builder
WORKDIR /src

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p dbtool-cli

FROM ${RUNTIME_IMAGE} AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/dbtool /usr/local/bin/dbtool

USER 65532:65532
ENTRYPOINT ["dbtool"]
CMD ["--help"]
