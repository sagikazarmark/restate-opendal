# Restate service for [OpenDAL](https://opendal.apache.org)

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/sagikazarmark/restate-opendal/dagger.yaml?style=flat-square)](https://github.com/sagikazarmark/restate-opendal/actions/workflows/dagger.yaml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/sagikazarmark/restate-opendal/badge?style=flat-square)](https://securityscorecards.dev/viewer/?uri=github.com/sagikazarmark/restate-opendal)
[![crates.io](https://img.shields.io/crates/v/restate-opendal?style=flat-square)](https://crates.io/crates/restate-opendal)
[![docs.rs](https://img.shields.io/docsrs/restate-opendal?style=flat-square)](https://docs.rs/restate-opendal)

**Restate service for accessing storage through [OpenDAL](https://opendal.apache.org).**

## Quick Start

Add the library to your application:

```toml
[dependencies]
restate-opendal = "0.11"
```

See the [`restate-opendal` Quick Start](crates/restate-opendal/README.md#quick-start) for endpoint setup examples and the service API.

## Packages

| Package | Description |
|---------|-------------|
| [`restate-opendal`](crates/restate-opendal/) | Library for adding OpenDAL storage operations to Restate endpoints |
| [`restate-opendal-endpoint`](crates/restate-opendal-endpoint/) | Ready-to-use endpoint hosting the OpenDAL services |

## Standalone Server

Run the server image and register its endpoint with Restate:

```bash
docker run -p 9080:9080 ghcr.io/sagikazarmark/restate-opendal:latest
restate deployments register http://localhost:9080
```

See the [`restate-opendal-endpoint` README](crates/restate-opendal-endpoint/README.md) for configuration options.

## Development

Minimum verification:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

Or run the same checks and the endpoint discovery test in containers with [Dagger](https://dagger.io):

- `dagger check`

## License

The project is licensed under the [MIT License](LICENSE).
