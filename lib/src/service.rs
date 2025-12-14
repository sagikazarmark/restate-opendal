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
