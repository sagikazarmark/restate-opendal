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

include!("service_common.rs");
