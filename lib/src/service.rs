use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_presign_response())]
pub struct PresignResponse {
    pub method: String,
    pub uri: String,
    pub headers: HashMap<String, String>,
}

fn example_presign_response() -> PresignResponse {
    PresignResponse {
        method: "GET".to_string(),
        uri: "https://example.com/path/to/file".to_string(),
        headers: HashMap::new(),
    }
}

impl From<opendal::raw::PresignedRequest> for PresignResponse {
    fn from(req: opendal::raw::PresignedRequest) -> Self {
        Self {
            method: req.method().to_string(),
            uri: req.uri().to_string(),
            headers: req
                .header()
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_string(),
                        v.to_str().unwrap_or_default().to_string(),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BytesRange {
    /// Offset of the range.
    offset: u64,
    /// Size of the range.
    size: Option<u64>,
}

impl From<BytesRange> for opendal::raw::BytesRange {
    fn from(def: BytesRange) -> Self {
        opendal::raw::BytesRange::new(def.offset, def.size)
    }
}

/// Options for read operations.
#[derive(Default, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReadOptions {
    /// Set `range` for this operation.
    ///
    /// If we have a file with size `n`.
    ///
    /// - `..` means read bytes in range `[0, n)` of file.
    /// - `0..1024` and `..1024` means read bytes in range `[0, 1024)` of file
    /// - `1024..` means read bytes in range `[1024, n)` of file
    ///
    /// The type implements `From<RangeBounds<u64>>`, so users can use `(1024..).into()` instead.
    #[serde(default)]
    pub range: BytesRange,
    /// Set `version` for this operation.
    ///
    /// This option can be used to retrieve the data of a specified version of the given path.
    ///
    /// If the version doesn't exist, an error with kind [`ErrorKind::NotFound`] will be returned.
    pub version: Option<String>,

    /// Set `if_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` matches the given `ETag`.
    ///
    /// If file exists and it's etag doesn't match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_match: Option<String>,
    /// Set `if_none_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` doesn't match the given `ETag`.
    ///
    /// If file exists and it's etag match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_none_match: Option<String>,
    /// Set `if_modified_since` for this operation.
    ///
    /// This option can be used to check if the file has been modified since the given timestamp.
    ///
    /// If file exists and it hasn't been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_modified_since: Option<jiff::Timestamp>,
    /// Set `if_unmodified_since` for this operation.
    ///
    /// This feature can be used to check if the file hasn't been modified since the given timestamp.
    ///
    /// If file exists and it has been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_unmodified_since: Option<jiff::Timestamp>,
    /// Set `concurrent` for the operation.
    ///
    /// OpenDAL by default to read file without concurrent. This is not efficient for cases when users
    /// read large chunks of data. By setting `concurrent`, opendal will reading files concurrently
    /// on support storage services.
    ///
    /// By setting `concurrent`, opendal will fetch chunks concurrently with
    /// the give chunk size.
    ///
    /// Refer to [`crate::docs::performance`] for more details.
    #[serde(default)]
    pub concurrent: usize,
    /// Set `chunk` for the operation.
    ///
    /// OpenDAL will use services' preferred chunk size by default. Users can set chunk based on their own needs.
    ///
    /// Refer to [`crate::docs::performance`] for more details.
    pub chunk: Option<usize>,
    /// Controls the optimization strategy for range reads in [`Reader::fetch`].
    ///
    /// When performing range reads, if the gap between two requested ranges is smaller than
    /// the configured `gap` size, OpenDAL will merge these ranges into a single read request
    /// and discard the unrequested data in between. This helps reduce the number of API calls
    /// to remote storage services.
    ///
    /// This optimization is particularly useful when performing multiple small range reads
    /// that are close to each other, as it reduces the overhead of multiple network requests
    /// at the cost of transferring some additional data.
    ///
    /// Refer to [`crate::docs::performance`] for more details.
    pub gap: Option<usize>,

    /// Specify the content-type header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_type: Option<String>,
    /// Specify the `cache-control` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_cache_control: Option<String>,
    /// Specify the `content-disposition` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_disposition: Option<String>,
}

impl From<ReadOptions> for opendal::options::ReadOptions {
    fn from(options: ReadOptions) -> Self {
        Self {
            range: options.range.into(),
            version: options.version,
            if_match: options.if_match,
            if_none_match: options.if_none_match,
            if_modified_since: options.if_modified_since.map(|t| t.into()),
            if_unmodified_since: options.if_unmodified_since.map(|t| t.into()),
            concurrent: options.concurrent,
            chunk: options.chunk,
            gap: options.gap,
            override_content_type: options.override_content_type,
            override_cache_control: options.override_cache_control,
            override_content_disposition: options.override_content_disposition,
        }
    }
}

/// Options for stat operations.
#[derive(Default, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StatOptions {
    /// Set `version` for this operation.
    ///
    /// This options can be used to retrieve the data of a specified version of the given path.
    ///
    /// If the version doesn't exist, an error with kind [`ErrorKind::NotFound`] will be returned.
    pub version: Option<String>,

    /// Set `if_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` matches the given `ETag`.
    ///
    /// If file exists and it's etag doesn't match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_match: Option<String>,
    /// Set `if_none_match` for this operation.
    ///
    /// This option can be used to check if the file's `ETag` doesn't match the given `ETag`.
    ///
    /// If file exists and it's etag match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    pub if_none_match: Option<String>,
    /// Set `if_modified_since` for this operation.
    ///
    /// This option can be used to check if the file has been modified since the given timestamp.
    ///
    /// If file exists and it hasn't been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_modified_since: Option<jiff::Timestamp>,
    /// Set `if_unmodified_since` for this operation.
    ///
    /// This feature can be used to check if the file hasn't been modified since the given timestamp.
    ///
    /// If file exists and it has been modified since the specified time, an error with kind
    /// [`ErrorKind::ConditionNotMatch`] will be returned.
    pub if_unmodified_since: Option<jiff::Timestamp>,

    /// Specify the content-type header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_type: Option<String>,
    /// Specify the `cache-control` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_cache_control: Option<String>,
    /// Specify the `content-disposition` header that should be sent back by the operation.
    ///
    /// This option is only meaningful when used along with presign.
    pub override_content_disposition: Option<String>,
}

impl From<StatOptions> for opendal::options::StatOptions {
    fn from(options: StatOptions) -> Self {
        Self {
            version: options.version,
            if_match: options.if_match,
            if_none_match: options.if_none_match,
            if_modified_since: options.if_modified_since.map(|t| t.into()),
            if_unmodified_since: options.if_unmodified_since.map(|t| t.into()),
            override_content_type: options.override_content_type,
            override_cache_control: options.override_cache_control,
            override_content_disposition: options.override_content_disposition,
        }
    }
}
