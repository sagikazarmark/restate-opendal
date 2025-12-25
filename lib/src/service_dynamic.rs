use restate_sdk::prelude::*;
use url::Url;

pub use crate::service::*;
use crate::{OperatorFactory, error::Error, service};

pub type Location = Url;

pub struct ServiceImpl {
    factory: OperatorFactory,
}

impl ServiceImpl {
    pub fn new(factory: OperatorFactory) -> Self {
        Self { factory }
    }
}

macro_rules! handler_impl {
    ($name:ident, $response:ty) => {
        paste::paste! {
            impl ServiceImpl {
                async fn [<_ $name:snake>](&self, request: [<$name:camel Request>]) -> Result<$response, Error> {
                    let (uri, path) = parse_uri(request.location.clone());

                    let operator = self.factory.from_uri(uri.as_str())?;

                    service::$name(&operator, path.as_str(), request).await
                }
            }
        }
    };

    ($name:ident) => {
        paste::paste! {
            handler_impl!($name, [<$name:camel Response>]);
        }
    };
}

fn parse_uri(uri: Url) -> (String, String) {
    let mut uri = uri;
    let path = uri.path().to_string();
    uri.set_path("");

    (uri.to_string(), path)
}

#[restate_sdk::service]
#[name = "OpenDAL"]
pub trait Service {
    /// List entries in a given location.
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

pub type ListRequest = service::ListRequest<Location>;
pub type PresignReadRequest = service::PresignRequest<Location, ReadOptions>;
pub type PresignStatRequest = service::PresignRequest<Location, StatOptions>;

handler_impl!(list);
handler_impl!(presign_read, PresignResponse);
handler_impl!(presign_stat, PresignResponse);

impl Service for ServiceImpl {
    /// List entries in a given location.
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
