FROM --platform=$BUILDPLATFORM tonistiigi/xx:1.9.0@sha256:c64defb9ed5a91eacb37f96ccc3d4cd72521c4bd18d5442905b95e2226b0e707 AS xx

FROM --platform=$BUILDPLATFORM rust:1.95.0-slim@sha256:72e5ceb0b77211cd46e45d327e33c12f2ad24b6f48ce54a279e07f95f21382f9 AS builder

COPY --from=xx / /

RUN apt-get update && apt-get install -y clang lld

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY bin/Cargo.toml ./bin/
COPY lib/Cargo.toml ./lib/

RUN mkdir -p bin/src && echo "fn main() {}" > bin/src/main.rs
RUN mkdir -p lib/src && echo "// dummy" > lib/src/lib.rs

RUN cargo fetch --locked

ARG TARGETPLATFORM

RUN xx-apt-get update && \
    xx-apt-get install -y \
    gcc \
    g++ \
    libc6-dev \
    pkg-config

COPY . ./

ARG RESTATE_SERVICE_NAME

RUN xx-cargo build --release --bin restate-opendal
RUN xx-verify ./target/$(xx-cargo --print-target-triple)/release/restate-opendal
RUN cp -r ./target/$(xx-cargo --print-target-triple)/release/restate-opendal /usr/local/bin/restate-opendal


# FROM alpine:3.23.0@sha256:51183f2cfa6320055da30872f211093f9ff1d3cf06f39a0bdb212314c5dc7375
FROM debian:13.4-slim@sha256:cedb1ef40439206b673ee8b33a46a03a0c9fa90bf3732f54704f99cb061d2c5a

COPY --from=builder /usr/local/bin/restate-opendal /usr/local/bin/

ENV RUST_LOG=info

EXPOSE 9080

CMD ["restate-opendal"]
