use std::{convert::TryFrom, time::Duration};

use anyhow::Result;
use restate_sdk::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{OperatorFactory, error::Error, service::*};

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
    /// URI to the object.
    pub uri: Url,
    /// Expiration of the presigned URL.
    #[serde(default, with = "humantime_serde")]
    #[schemars(with = "Option<String>")]
    pub expiration: Duration,
}

fn example_presign_request() -> PresignRequest {
    PresignRequest {
        uri: Url::parse("https://example.com/path/to/file.pdf").unwrap(),
        expiration: Duration::from_secs(3600),
    }
}

pub struct OpendalImpl {
    factory: OperatorFactory,
}

impl OpendalImpl {
    pub fn new(factory: OperatorFactory) -> Self {
        Self { factory }
    }

    async fn _presign_read(&self, request: PresignRequest) -> Result<PresignResponse, Error> {
        let (uri, path) = parse_uri(request.uri);

        let operator = self.factory.from_uri(uri.as_str())?;

        Ok(operator
            .presign_read(path.as_str(), request.expiration)
            .await?
            .into())
    }
}

impl Opendal for OpendalImpl {
    /// Presign an operation for read.
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

fn parse_uri(uri: Url) -> (String, String) {
    let mut uri = uri;
    let path = uri.path().to_string();
    uri.set_path("");

    (uri.to_string(), path)
}
