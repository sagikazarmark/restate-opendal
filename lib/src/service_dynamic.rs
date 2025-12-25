use std::{convert::TryFrom, time::Duration};

use anyhow::Result;
use restate_sdk::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{OperatorFactory, error::Error, service, service::*};

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
    factory: OperatorFactory,
}

impl ServiceImpl {
    pub fn new(factory: OperatorFactory) -> Self {
        Self { factory }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_list_request())]
pub struct ListRequest {
    /// Object URI.
    pub uri: Url,
    #[serde(flatten)]
    common: service::CommonListRequest,
}

fn example_list_request() -> ListRequest {
    ListRequest {
        uri: Url::parse("s3://bucket").unwrap(),
        common: service::CommonListRequest { options: None },
    }
}

impl ServiceImpl {
    async fn _list(&self, request: ListRequest) -> Result<ListResponse, Error> {
        let (uri, path) = parse_uri(request.uri);

        let operator = self.factory.from_uri(uri.as_str())?;

        service::list(&operator, path.as_str(), request.common).await
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_presign_read_request())]
pub struct PresignReadRequest {
    /// Object URI.
    pub uri: Url,
    #[serde(flatten)]
    common: service::PresignRequest<ReadOptions>,
}

fn example_presign_read_request() -> PresignReadRequest {
    PresignReadRequest {
        uri: Url::parse("https://example.com/path/to/file.pdf").unwrap(),
        common: service::PresignRequest::<ReadOptions> {
            expiration: Duration::from_secs(3600),
            options: None,
        },
    }
}

impl ServiceImpl {
    async fn _presign_read(&self, request: PresignReadRequest) -> Result<PresignResponse, Error> {
        let (uri, path) = parse_uri(request.uri);

        let operator = self.factory.from_uri(uri.as_str())?;

        presign_read(&operator, path.as_str(), request.common).await
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_presign_stat_request())]
pub struct PresignStatRequest {
    /// Object URI.
    pub uri: Url,
    #[serde(flatten)]
    common: service::PresignRequest<StatOptions>,
}

fn example_presign_stat_request() -> PresignStatRequest {
    PresignStatRequest {
        uri: Url::parse("https://example.com/path/to/file.pdf").unwrap(),
        common: service::PresignRequest::<StatOptions> {
            expiration: Duration::from_secs(3600),
            options: None,
        },
    }
}

impl ServiceImpl {
    async fn _presign_stat(&self, request: PresignStatRequest) -> Result<PresignResponse, Error> {
        let (uri, path) = parse_uri(request.uri);

        let operator = self.factory.from_uri(uri.as_str())?;

        presign_stat(&operator, path.as_str(), request.common).await
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

fn parse_uri(uri: Url) -> (String, String) {
    let mut uri = uri;
    let path = uri.path().to_string();
    uri.set_path("");

    (uri.to_string(), path)
}
