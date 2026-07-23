mod config;
mod config_restate;

use std::{collections::HashMap, path::PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use figment::{
    Figment,
    providers::{Env, Format, Json, Toml, Yaml},
};
use opendal::{
    OperatorRegistry,
    layers::{LoggingLayer, MimeGuessLayer, TracingLayer},
    services,
};
use opendal_util::{
    ChainOperatorFactory, DefaultOperatorFactory, LambdaOperatorFactory, OperatorFactory,
    ProfileOperatorFactory,
};
use restate_sdk::{endpoint::Endpoint, http_server::HttpServer, service::IntoServiceDefinition};

use restate_opendal::{dynamic, extra, scoped};

use crate::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    let config = cli.load_config()?;

    opendal::init_default_registry();
    // OpenDAL registers the HTTP backend for "http", but it also supports HTTPS URLs.
    OperatorRegistry::get().register::<services::Http>("https");

    let mut endpoint = Endpoint::builder();

    {
        let factory = create_factory(config.profiles.clone());

        if let Some(store_url) = config.store.uri {
            let operator = factory.load(store_url.as_str())?;
            let service = scoped::ServiceImpl::new(operator)
                .into_service_definition()
                .options(config.restate.service.into());

            endpoint = endpoint.bind(service)
        } else {
            let service = dynamic::ServiceImpl::new(factory)
                .into_service_definition()
                .options(config.restate.service.into());

            endpoint = endpoint.bind(service)
        }
    }

    {
        let factory = create_factory(config.profiles.clone());

        endpoint = endpoint.bind(extra::ServiceImpl::new(factory));
    }

    let bind_addr = format!("0.0.0.0:{}", cli.port);

    // Create and start the HTTP server
    HttpServer::new(endpoint.build())
        .listen_and_serve(bind_addr.parse()?)
        .await;

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version)]
struct Cli {
    /// Path to config file (supports JSON, YAML, or TOML)
    #[arg(long, value_name = "FILE", env = "CONFIG_FILE")]
    config: Option<PathBuf>,

    /// Port to listen on
    #[arg(long, default_value = "9080", env = "PORT")]
    port: u16,
}

impl Cli {
    fn load_config(&self) -> Result<Config> {
        let mut figment = Figment::new();

        if let Some(path) = self.config.as_deref() {
            if !path.exists() {
                anyhow::bail!("Config file not found: {}", path.display());
            }

            figment = match path.extension().and_then(|s| s.to_str()) {
                Some("toml") => figment.merge(Toml::file(path)),
                Some("json") => figment.merge(Json::file(path)),
                Some("yaml") | Some("yml") => figment.merge(Yaml::file(path)),
                _ => anyhow::bail!(
                    "Unsupported config file format. Use .toml, .json, .yaml, or .yml"
                ),
            };
        }

        figment = figment.merge(Env::raw().split("__")).merge(
            Env::prefixed("OPENDAL_")
                .filter(|k| k.starts_with("profile_"))
                .map(move |key| key.as_str().replacen("_", ".", 2).into()),
            // .split("_"),
        );

        figment.extract().context("Failed to parse configuration")
    }
}

fn create_factory(profiles: HashMap<String, HashMap<String, String>>) -> impl OperatorFactory {
    LambdaOperatorFactory::new(
        ChainOperatorFactory::builder()
            .then(ProfileOperatorFactory::new(profiles))
            .then(DefaultOperatorFactory::new())
            .build(),
        |o| {
            o.layer(LoggingLayer::default())
                .layer(TracingLayer::new())
                .layer(MimeGuessLayer::default())
        },
    )
}
