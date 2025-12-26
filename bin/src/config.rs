use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::config_restate::*;

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct Config {
    #[serde(default)]
    pub restate: RestateConfig,

    #[serde(default)]
    pub store: StoreConfig,

    #[serde(default, alias = "profile")]
    pub profiles: HashMap<String, HashMap<String, String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct StoreConfig {
    #[serde(default)]
    pub uri: Option<Url>,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct RestateConfig {
    #[serde(default)]
    pub service: ServiceOptionsConfig,
}
