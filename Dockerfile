# syntax=docker/dockerfile:1
# check=skip=CopyIgnoredFile

FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.9.0@sha256:c64defb9ed5a91eacb37f96ccc3d4cd72521c4bd18d5442905b95e2226b0e707 AS xx

FROM --platform=$BUILDPLATFORM rust:1.98.0-bookworm@sha256:e70e2eec3d495fd5c8e0be74adda86507dfac7f51a724fbf9813ff59b2b247c7 AS base

ARG CARGO_CHEF_VERSION=0.1.77
RUN cargo install cargo-chef --version $CARGO_CHEF_VERSION --locked

COPY --from=xx / /

WORKDIR /usr/src/app


FROM base AS deps

COPY . .

RUN cargo chef prepare --recipe-path recipe.json


FROM base AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends clang lld && \
    rm -rf /var/lib/apt/lists/*

ARG TARGETPLATFORM

RUN xx-apt-get update && \
    xx-apt-get install -y --no-install-recommends gcc libc6-dev && \
    rm -rf /var/lib/apt/lists/*

COPY --from=deps /usr/src/app/recipe.json recipe.json

RUN xx-cargo chef cook --locked --release --recipe-path recipe.json

COPY . .

RUN xx-cargo build --locked --release --bin restate-opendal
RUN xx-verify ./target/$(xx-cargo --print-target-triple)/release/restate-opendal
RUN cp ./target/$(xx-cargo --print-target-triple)/release/restate-opendal /usr/local/bin/restate-opendal


FROM debian:13.6-slim@sha256:020c0d20b9880058cbe785a9db107156c3c75c2ac944a6aa7ab59f2add76a7bd

COPY --from=builder /usr/local/bin/restate-opendal /usr/local/bin/

ENV RUST_LOG=info

EXPOSE 9080

USER 65532:65532

CMD ["restate-opendal"]
