use anyhow::Result;
use content_disposition::parse_content_disposition;
use futures::TryStreamExt;
use opendal::{ErrorKind, Operator};
use restate_sdk::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use typed_path::UnixPath;
use url::Url;

use crate::OperatorFactory;
use crate::terminal;

#[restate_sdk::service]
#[name = "OpenDALExtra"]
pub trait OpendalExtra {
    /// Copy a file from one location to another.
    async fn copy(request: Json<CopyRequest>) -> HandlerResult<()>;
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_copy_request())]
pub struct CopyRequest {
    pub source: Url,
    pub destination: Url,
}

fn example_copy_request() -> CopyRequest {
    CopyRequest {
        source: Url::parse(
            "https://download.blender.org/peach/bigbuckbunny_movies/big_buck_bunny_1080p_h264.mov",
        )
        .unwrap(),
        destination: Url::parse("s3://bucket/bunny.mov").unwrap(),
    }
}

#[derive(Default)]
pub struct OpendalExtraImpl {
    factory: OperatorFactory,
}

impl OpendalExtraImpl {
    pub fn new(factory: OperatorFactory) -> Self {
        Self { factory }
    }

    async fn _copy(&self, request: CopyRequest) -> Result<(), crate::error::Error> {
        let (src_path, src_op) = self.parse_location(request.source)?;
        let (dst_path, dst_op) = self.parse_location(request.destination)?;

        let src_stat = src_op.stat(src_path.as_str()).await?;
        if src_stat.is_dir() {
            // op_from.info().native_capability().list;
            terminal!("Copying directories is not supported (yet)");
        }

        let real_dst_path = match dst_op.stat(&dst_path).await {
            Ok(stat) if stat.is_dir() => {
                // Destination exists and is a directory
                if let Some(filename) = UnixPath::new(&src_path).file_name() {
                    UnixPath::new(&dst_path)
                        .join(filename)
                        .to_string_lossy()
                        .to_string()
                } else if let Some(filename) = src_stat
                    .content_disposition()
                    .and_then(|cd| parse_content_disposition(cd).filename_full())
                {
                    filename
                } else {
                    terminal!(
                        "Cannot copy source '{}' into directory '{}': Source has no filename.",
                        src_path,
                        dst_path
                    );
                }
            }
            Ok(_) => {
                // Destination exists and is a file (overwrite)
                dst_path.clone()
            }
            Err(e) if e.kind() == ErrorKind::NotFound => dst_path.clone(),
            Err(e) => {
                return Err(e.into());
            }
        };

        let reader = src_op.reader(src_path.as_str()).await?;
        let mut writer_builder = dst_op.writer_with(real_dst_path.as_str());

        if let Some(content_type) = src_stat.content_type() {
            writer_builder = writer_builder.content_type(content_type);
        }
        // TODO: add other metadata?

        let mut writer = writer_builder.await?;

        let mut stream = reader.into_bytes_stream(..).await?;
        while let Some(chunk) = stream.try_next().await? {
            writer.write(chunk).await?;
        }

        writer.close().await?;

        Ok(())
    }

    fn parse_location(&self, location: Url) -> Result<(String, Operator)> {
        let mut uri = location;
        let path = uri.path().to_string();
        uri.set_path("");

        let op = self.factory.from_uri(uri.as_str())?;

        Ok((path, op))
    }
}

impl OpendalExtra for OpendalExtraImpl {
    /// Copy a file from one location to another.
    async fn copy(&self, ctx: Context<'_>, request: Json<CopyRequest>) -> HandlerResult<()> {
        ctx.run(async || Ok(self._copy(request.into_inner()).await?))
            .await?;

        Ok(())
    }
}
