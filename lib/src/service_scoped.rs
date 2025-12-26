use opendal::Operator;
use restate_sdk::prelude::*;

pub use crate::service::*;
use crate::{error::Error, service};

pub type Location = String;

pub struct ServiceImpl {
    operator: Operator,
}

impl ServiceImpl {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

macro_rules! handler_impl {
    ($name:ident, $response:ty) => {
        paste::paste! {
            impl ServiceImpl {
                async fn [<_ $name:snake>](&self, request: [<$name:camel Request>]) -> Result<$response, Error> {
                    service::$name(&self.operator, request.location.clone().as_str(), request).await
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

include!("service_common.rs");

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
