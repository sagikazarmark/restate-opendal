# restate-opendal-endpoint

**Standalone endpoint hosting OpenDAL storage services for [Restate](https://restate.dev/).**

## Install

Pre-built container images are available from GitHub Container Registry:

```bash
docker pull ghcr.io/sagikazarmark/restate-opendal:latest
```

Or install from source:

```bash
cargo install restate-opendal-endpoint
```

## Quick Start

Run the server and register it with Restate:

```bash
restate-opendal --port 9080
restate deployments register http://localhost:9080
```

Without a configured store, requests select an OpenDAL operator dynamically by URL. Set `store.uri` to expose a service scoped to one operator.

## Configuration

Pass configuration through CLI arguments:

```text
--config <FILE>    Configuration file path
--port <PORT>      Listen port (default: 9080)
```

The endpoint reads `CONFIG_FILE`, `PORT`, and `RUST_LOG`. Configuration files may use JSON, YAML, or TOML:

```toml
[store]
uri = "s3://bucket"

[profiles.default]
access_key_id = "..."
secret_access_key = "..."
endpoint = "http://localhost:9000"
```

Restate service options can be set below `[restate.service]`, including timeouts, retention, retry policy, metadata, and handler-specific overrides.

## License

The project is licensed under the [MIT License](../../LICENSE).
