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

macro_rules! handler_impl {
    ($name:ident, $common:ty, $response:ty) => {
        paste::paste! {
            #[derive(Debug, Deserialize, Serialize, JsonSchema)]
            #[serde(rename_all = "camelCase")]
            #[schemars(example = [<example_ $name:snake _request>]())]
            pub struct [<$name:camel Request>] {
                /// Object URI.
                pub uri: Url,
                #[serde(flatten)]
                common: $common,
            }

            fn [<example_ $name:snake _request>]() -> [<$name:camel Request>] {
                [<$name:camel Request>] {
                    uri: Url::parse("https://example.com/path/to/file.pdf").unwrap(),
                    common: $common::example(),
                }
            }

            impl ServiceImpl {
                async fn [<_ $name:snake>](&self, request: [<$name:camel Request>]) -> Result<$response, Error> {
                    let (uri, path) = parse_uri(request.uri);

                    let operator = self.factory.from_uri(uri.as_str())?;

                    service::$name(&operator, path.as_str(), request.common).await
                }
            }
        }
    };

    ($name:ident, $common:ty) => {
        paste::paste! {
            handler_impl!($name, $common, [<$name:camel Response>]);
        }
    };
}

handler_impl!(list, service::CommonListRequest);
handler_impl!(
    presign_read,
    service::PresignRequest::<ReadOptions>,
    service::PresignResponse
);
handler_impl!(
    presign_stat,
    service::PresignRequest::<StatOptions>,
    service::PresignResponse
);

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
