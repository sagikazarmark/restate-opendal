use std::{convert::TryFrom, time::Duration};

use anyhow::Result;
use futures::{StreamExt, TryStreamExt};
use restate_sdk::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{OperatorFactory, error::Error, service::*};

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

macro_rules! presign_request {
    ($name:ident) => {
        paste::paste! {
            #[derive(Debug, Deserialize, Serialize, JsonSchema)]
            #[serde(rename_all = "camelCase")]
            #[schemars(example = [<example_presign_ $name:snake _request>]())]
            pub struct [<Presign $name Request>] {
                /// Object URI.
                pub uri: Url,
                /// Expiration of the presigned URL.
                #[serde(with = "humantime_serde")]
                #[schemars(with = "String")]
                pub expiration: Duration,
                /// Options for the presigned URL.
                #[serde(skip_serializing_if = "Option::is_none")]
                pub options: Option<[<$name Options>]>,
            }

            fn [<example_presign_ $name:snake _request>]() -> [<Presign $name Request>] {
                [<Presign $name Request>] {
                    uri: Url::parse("https://example.com/path/to/file.pdf").unwrap(),
                    expiration: Duration::from_secs(3600),
                    options: None,
                }
            }
        }
    };
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_list_request())]
pub struct ListRequest {
    /// Object URI.
    pub uri: Url,
    /// Options for the presigned URL.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "option_list_options"
    )]
    #[schemars(with = "Option<ListOptionsDef>")]
    pub options: Option<opendal::options::ListOptions>,
}

fn example_list_request() -> ListRequest {
    ListRequest {
        uri: Url::parse("s3://bucket").unwrap(),
        options: None,
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_list_response())]
pub struct ListResponse {
    /// Entries in the store.
    pub entries: Vec<Entry>,
}

fn example_list_response() -> ListResponse {
    ListResponse { entries: vec![] }
}

presign_request!(Read);
presign_request!(Stat);
// presign_request!(Write);
// presign_request!(Delete);

pub struct ServiceImpl {
    factory: OperatorFactory,
}

impl ServiceImpl {
    pub fn new(factory: OperatorFactory) -> Self {
        Self { factory }
    }

    async fn _list(&self, request: ListRequest) -> Result<ListResponse, Error> {
        let (uri, path) = parse_uri(request.uri);

        let operator = self.factory.from_uri(uri.as_str())?;

        let lister;

        if let Some(options) = request.options {
            lister = operator.lister_options(path.as_str(), options).await?;
        } else {
            lister = operator.lister(path.as_str()).await?;
        }

        let entries: Vec<Entry> = lister
            .map(|entry| entry.map(|e| e.into()))
            .try_collect()
            .await?;

        Ok(ListResponse { entries })
    }

    async fn _presign_read(&self, request: PresignReadRequest) -> Result<PresignResponse, Error> {
        let (uri, path) = parse_uri(request.uri);

        let operator = self.factory.from_uri(uri.as_str())?;

        if let Some(options) = request.options {
            Ok(operator
                .presign_read_options(path.as_str(), request.expiration, options.into())
                .await?
                .into())
        } else {
            Ok(operator
                .presign_read(path.as_str(), request.expiration)
                .await?
                .into())
        }
    }

    async fn _presign_stat(&self, request: PresignStatRequest) -> Result<PresignResponse, Error> {
        let (uri, path) = parse_uri(request.uri);

        let operator = self.factory.from_uri(uri.as_str())?;

        if let Some(options) = request.options {
            Ok(operator
                .presign_stat_options(path.as_str(), request.expiration, options.into())
                .await?
                .into())
        } else {
            Ok(operator
                .presign_stat(path.as_str(), request.expiration)
                .await?
                .into())
        }
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
