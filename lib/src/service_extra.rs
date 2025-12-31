use opendal::Operator;
use opendal_util::{Copier, CopyOptions, OperatorFactory};
use restate_sdk::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use url::Url;

#[restate_sdk::service]
#[name = "OpenDALExtra"]
pub trait Service {
    /// Copy a file from one location to another.
    async fn copy(request: Json<CopyRequest>) -> HandlerResult<()>;
}

#[derive(Default)]
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

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_copy_request())]
pub struct CopyRequest {
    pub source: Url,
    pub destination: Url,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<CopyOptions>,
}

fn example_copy_request() -> CopyRequest {
    CopyRequest {
        source: Url::parse(
            "https://download.blender.org/peach/bigbuckbunny_movies/big_buck_bunny_1080p_h264.mov",
        )
        .unwrap(),
        destination: Url::parse("s3://bucket/bunny.mov").unwrap(),
        options: None,
    }
}

impl<F> ServiceImpl<F>
where
    F: OperatorFactory,
{
    async fn _copy(&self, request: CopyRequest) -> opendal::Result<()> {
        let (src_path, src_op) = self.parse_location(request.source)?;
        let (dst_path, dst_op) = self.parse_location(request.destination)?;

        let copier = Copier::new(src_op, dst_op);

        if let Some(options) = request.options {
            return copier.copy_options(src_path, dst_path, options).await;
        }

        copier.copy(src_path, dst_path).await
    }

    fn parse_location(&self, location: Url) -> opendal::Result<(String, Operator)> {
        let mut uri = location;
        let path = uri.path().to_string();
        uri.set_path("");

        let op = self.factory.load(uri.as_str())?;

        Ok((path, op))
    }
}

impl<F> Service for ServiceImpl<F>
where
    F: OperatorFactory,
{
    /// Copy a file from one location to another.
    async fn copy(&self, ctx: Context<'_>, request: Json<CopyRequest>) -> HandlerResult<()> {
        ctx.run(async || Ok(self._copy(request.into_inner()).await?))
            .await?;

        Ok(())
    }
}
