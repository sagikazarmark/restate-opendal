use std::{convert::TryFrom, time::Duration};

use anyhow::Result;
use opendal::Operator;
use restate_sdk::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{error::Error, service, service::*};

#[restate_sdk::service]
#[name = "OpenDAL"]
pub trait Service {
    /// List entries in a given path.
    async fn list(request: Json<ListRequest>) -> HandlerResult<Json<ListResponse>>;

    /// Presign an operation for read.
    #[name = "presignRead"]
    async fn presign_read(
        request: Json<PresignReadRequest>,
    ) -> HandlerResult<Json<PresignResponse>>;

    /// Presign an operation for read.
    #[name = "presignStat"]
    async fn presign_stat(
        request: Json<PresignStatRequest>,
    ) -> HandlerResult<Json<PresignResponse>>;
}

pub struct ServiceImpl {
    operator: Operator,
}

impl ServiceImpl {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_list_request())]
pub struct ListRequest {
    /// Object path.
    pub path: String,
    #[serde(flatten)]
    common: service::CommonListRequest,
}

fn example_list_request() -> ListRequest {
    ListRequest {
        path: "path/to/file.pdf".to_string(),
        common: service::CommonListRequest { options: None },
    }
}

impl ServiceImpl {
    async fn _list(&self, request: ListRequest) -> Result<ListResponse, Error> {
        service::list(&self.operator, request.path.as_str(), request.common).await
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_presign_read_request())]
pub struct PresignReadRequest {
    /// Object path.
    pub path: String,
    #[serde(flatten)]
    common: service::PresignRequest<ReadOptions>,
}

fn example_presign_read_request() -> PresignReadRequest {
    PresignReadRequest {
        path: "path/to/file.pdf".to_string(),
        common: service::PresignRequest::<ReadOptions> {
            expiration: Duration::from_secs(3600),
            options: None,
        },
    }
}

impl ServiceImpl {
    async fn _presign_read(&self, request: PresignReadRequest) -> Result<PresignResponse, Error> {
        presign_read(&self.operator, request.path.as_str(), request.common).await
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_presign_stat_request())]
pub struct PresignStatRequest {
    /// Object path.
    pub path: String,
    #[serde(flatten)]
    common: service::PresignRequest<StatOptions>,
}

fn example_presign_stat_request() -> PresignStatRequest {
    PresignStatRequest {
        path: "path/to/file.pdf".to_string(),
        common: service::PresignRequest::<StatOptions> {
            expiration: Duration::from_secs(3600),
            options: None,
        },
    }
}

impl ServiceImpl {
    async fn _presign_stat(&self, request: PresignStatRequest) -> Result<PresignResponse, Error> {
        presign_stat(&self.operator, request.path.as_str(), request.common).await
    }
}

impl Service for ServiceImpl {
    /// List entries in a given path.
    async fn list(
        &self,
        ctx: Context<'_>,
        request: Json<ListRequest>,
    ) -> HandlerResult<Json<ListResponse>> {
        Ok(ctx
            .run(async || Ok(self._list(request.into_inner()).await.map(Json)?))
            .await?)
    }

    /// Presign an operation for read.
    async fn presign_read(
        &self,
        ctx: Context<'_>,
        request: Json<PresignReadRequest>,
    ) -> HandlerResult<Json<PresignResponse>> {
        Ok(ctx
            .run(async || Ok(self._presign_read(request.into_inner()).await.map(Json)?))
            .await?)
    }

    /// Presign an operation for stat.
    async fn presign_stat(
        &self,
        ctx: Context<'_>,
        request: Json<PresignStatRequest>,
    ) -> HandlerResult<Json<PresignResponse>> {
        Ok(ctx
            .run(async || Ok(self._presign_stat(request.into_inner()).await.map(Json)?))
            .await?)
    }
}
