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
