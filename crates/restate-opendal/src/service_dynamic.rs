use opendal_util::OperatorFactory;
use restate_sdk::prelude::*;
use url::Url;

pub use crate::service::*;
use crate::{error::Error, service};

pub type Location = Url;

pub struct ServiceImpl<F>
where
    F: OperatorFactory,
{
    factory: F,
}

impl<F> ServiceImpl<F>
where
    F: OperatorFactory,
{
    pub fn new(factory: F) -> Self {
        Self { factory }
    }
}

macro_rules! handler_impl {
    ($name:ident, $response:ty) => {
        pastey::paste! {
            impl<F> ServiceImpl<F>
            where
                F: OperatorFactory,
            {
                async fn [<_ $name:snake>](&self, request: [<$name:camel Request>]) -> Result<$response, Error> {
                    let (uri, path) = parse_uri(request.location.clone());

                    let operator = self.factory.load(uri.as_str())?;

                    service::$name(&operator, path.as_str(), request).await
                }
            }
        }
    };

    ($name:ident) => {
        pastey::paste! {
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

include!("service_common.rs");

#[restate_sdk::service(name = "OpenDAL")]
impl<F> ServiceImpl<F>
where
    F: OperatorFactory + 'static,
{
    /// List entries in a given location.
    #[handler]
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
    #[handler(name = "presignRead")]
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
    #[handler(name = "presignStat")]
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
