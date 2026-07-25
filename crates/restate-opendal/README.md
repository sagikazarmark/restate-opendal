# restate-opendal

[![crates.io](https://img.shields.io/crates/v/restate-opendal?style=flat-square)](https://crates.io/crates/restate-opendal)
[![docs.rs](https://img.shields.io/docsrs/restate-opendal?style=flat-square)](https://docs.rs/restate-opendal)

**OpenDAL storage services for [Restate](https://restate.dev/).**

The crate provides scoped and dynamic `OpenDAL` services plus the `OpenDALExtra` cross-storage service.

## Install

```bash
cargo add restate-opendal
```

## Quick Start

Bind a service backed by one OpenDAL operator:

```rust
use restate_opendal::scoped;
use restate_sdk::{endpoint::Endpoint, service::IntoServiceDefinition};

let operator = opendal::Operator::new(opendal::services::Memory::default())?.finish();
let endpoint = Endpoint::builder()
    .bind(scoped::ServiceImpl::new(operator).into_service_definition())
    .build();
```

Use `dynamic::ServiceImpl` with an `opendal_util::OperatorFactory` when each request should select storage by URL. `extra::ServiceImpl` adds operations spanning two operators.

## Services

| Service | Handlers | Description |
|---------|----------|-------------|
| `OpenDAL` | `list`, `presignRead`, `presignStat` | List and presign objects using scoped or URL-selected storage |
| `OpenDALExtra` | `copy` | Copy objects between storage locations |

## License

The project is licensed under the [MIT License](../../LICENSE).
