use std::{collections::HashMap, time::Duration};

use futures::{StreamExt, TryStreamExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommonListRequest {
    /// Options for the presigned URL.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "option_list_options"
    )]
    #[schemars(with = "Option<ListOptionsDef>")]
    pub options: Option<opendal::options::ListOptions>,
}

impl CommonListRequest {
    pub(crate) fn example() -> Self {
        Self { options: None }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(example = example_list_response())]
pub struct ListResponse {
    /// Entries in the store.
    pub entries: Vec<Entry>,
}

fn example_list_response() -> ListResponse {
    ListResponse { entries: vec![] }
}

pub(crate) async fn list(
    operator: &opendal::Operator,
    path: &str,
    request: CommonListRequest,
) -> Result<ListResponse, Error> {
    let lister;

    if let Some(options) = request.options {
        lister = operator.lister_options(path, options).await?;
    } else {
        lister = operator.lister(path).await?;
    }

    let entries: Vec<Entry> = lister
        .map(|entry| entry.map(|e| e.into()))
        .try_collect()
        .await?;

    Ok(ListResponse { entries })
}

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
pub struct PresignRequest<O> {
    /// Expiration of the presigned URL.
    #[serde(with = "humantime_serde")]
    #[schemars(with = "String")]
    pub expiration: Duration,
    /// Options for the presigned URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<O>,
}

impl<O> PresignRequest<O> {
    pub(crate) fn example() -> Self {
        Self {
            expiration: Duration::from_secs(3600),
            options: None,
        }
    }
}

pub(crate) async fn presign_read(
    operator: &opendal::Operator,
    path: &str,
    request: PresignRequest<ReadOptions>,
) -> Result<PresignResponse, Error> {
    let presigned;

    if let Some(options) = request.options {
        presigned = operator
            .presign_read_options(path, request.expiration, options.into())
            .await?;
    } else {
        presigned = operator.presign_read(path, request.expiration).await?;
    }

    Ok(presigned.into())
}

pub(crate) async fn presign_stat(
    operator: &opendal::Operator,
    path: &str,
    request: PresignRequest<StatOptions>,
) -> Result<PresignResponse, Error> {
    let presigned;

    if let Some(options) = request.options {
        presigned = operator
            .presign_stat_options(path, request.expiration, options.into())
            .await?;
    } else {
        presigned = operator.presign_stat(path, request.expiration).await?;
    }

    Ok(presigned.into())
}

#[derive(Default, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub path: String,
    pub metadata: Metadata,
}

impl From<opendal::Entry> for Entry {
    fn from(val: opendal::Entry) -> Self {
        Entry {
            path: val.path().to_string(),
            metadata: val.metadata().clone().into(),
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    pub mode: EntryMode,

    pub is_current: Option<bool>,
    pub is_deleted: bool,

    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_length: Option<u64>,
    pub content_md5: Option<String>,
    // pub content_range: Option<BytesContentRange>,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<jiff::Timestamp>,
    pub version: Option<String>,

    pub user_metadata: Option<HashMap<String, String>>,
}

impl From<opendal::Metadata> for Metadata {
    fn from(val: opendal::Metadata) -> Self {
        Metadata {
            mode: val.mode().into(),
            is_current: val.is_current(),
            is_deleted: val.is_deleted(),
            cache_control: val.cache_control().map(|s| s.to_string()),
            content_disposition: val.content_disposition().map(|s| s.to_string()),
            content_length: Some(val.content_length()),
            content_md5: val.content_md5().map(|s| s.to_string()),
            content_type: val.content_type().map(|s| s.to_string()),
            content_encoding: val.content_encoding().map(|s| s.to_string()),
            etag: val.etag().map(|s| s.to_string()),
            last_modified: val.last_modified().map(|t| t.into_inner()),
            version: val.version().map(|s| s.to_string()),
            user_metadata: val.user_metadata().cloned(),
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum EntryMode {
    /// FILE means the path has data to read.
    File,
    /// DIR means the path can be listed.
    Dir,
    /// Unknown means we don't know what we can do on this path.
    #[default]
    Unknown,
}

impl From<opendal::EntryMode> for EntryMode {
    fn from(val: opendal::EntryMode) -> Self {
        match val {
            opendal::EntryMode::FILE => EntryMode::File,
            opendal::EntryMode::DIR => EntryMode::Dir,
            opendal::EntryMode::Unknown => EntryMode::Unknown,
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", remote = "opendal::options::ListOptions")]
pub struct ListOptionsDef {
    /// The limit passed to underlying service to specify the max results
    /// that could return per-request.
    ///
    /// Users could use this to control the memory usage of list operation.
    #[serde(default)]
    pub limit: Option<usize>,
    /// The start_after passes to underlying service to specify the specified key
    /// to start listing from.
    #[serde(default)]
    pub start_after: Option<String>,
    /// The recursive is used to control whether the list operation is recursive.
    ///
    /// - If `false`, list operation will only list the entries under the given path.
    /// - If `true`, list operation will list all entries that starts with given path.
    ///
    /// Default to `false`.
    #[serde(default)]
    pub recursive: bool,
    /// The version is used to control whether the object versions should be returned.
    ///
    /// - If `false`, list operation will not return with object versions
    /// - If `true`, list operation will return with object versions if object versioning is supported
    ///   by the underlying service
    ///
    /// Default to `false`
    #[serde(default)]
    pub versions: bool,
    /// The deleted is used to control whether the deleted objects should be returned.
    ///
    /// - If `false`, list operation will not return with deleted objects
    /// - If `true`, list operation will return with deleted objects if object versioning is supported
    ///   by the underlying service
    ///
    /// Default to `false`
    #[serde(default)]
    pub deleted: bool,
}

pub(crate) mod option_list_options {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        value: &Option<opendal::options::ListOptions>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(
            #[serde(with = "super::ListOptionsDef")] &'a opendal::options::ListOptions,
        );

        value.as_ref().map(Helper).serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<Option<opendal::options::ListOptions>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "super::ListOptionsDef")] opendal::options::ListOptions);

        Option::<Helper>::deserialize(deserializer).map(|opt| opt.map(|Helper(inner)| inner))
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
