use std::{convert::TryFrom, time::Duration};

use anyhow::Result;
use opendal::Operator;
use restate_sdk::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{error::Error, service::*};

#[restate_sdk::service]
#[name = "OpenDAL"]
pub trait Opendal {
    /// Presign an operation for read.
    #[name = "presignRead"]
    async fn presign_read(request: Json<PresignRequest>) -> HandlerResult<Json<PresignResponse>>;
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_presign_request())]
pub struct PresignRequest {
    /// Path to the object.
    pub path: String,
    /// Expiration of the presigned URL.
    #[serde(default, with = "humantime_serde")]
    #[schemars(with = "Option<String>")]
    pub expiration: Duration,
}

fn example_presign_request() -> PresignRequest {
    PresignRequest {
        path: "path/to/file.pdf".to_string(),
        expiration: Duration::from_secs(3600),
    }
}

pub struct OpendalImpl {
    operator: Operator,
}

impl OpendalImpl {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    async fn _presign_read(&self, request: PresignRequest) -> Result<PresignResponse, Error> {
        Ok(self
            .operator
            .presign_read(request.path.as_str(), request.expiration)
            .await?
            .into())
    }
}

impl Opendal for OpendalImpl {
    async fn presign_read(
        &self,
        ctx: Context<'_>,
        request: Json<PresignRequest>,
    ) -> HandlerResult<Json<PresignResponse>> {
        Ok(ctx
            .run(async || Ok(self._presign_read(request.into_inner()).await.map(Json)?))
            .await?)
    }
}
