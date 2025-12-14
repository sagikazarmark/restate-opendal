mod config;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use figment::{
    Figment,
    providers::{Env, Format, Json, Toml, Yaml},
};
use opendal::{DEFAULT_OPERATOR_REGISTRY, layers::LoggingLayer, services};
use restate_sdk::{endpoint::Endpoint, http_server::HttpServer};

use restate_opendal::{LambdaOperatorFactory, OperatorFactory};
use restate_opendal::{dynamic, dynamic::Service as _, scoped, scoped::Service as _};
use restate_opendal::{extra, extra::Service as _};

use crate::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt::init();

    let config = cli.load_config()?;

    print!("Config: {:?}", config);

    // Register HTTP scheme (for some reason these are not registered by default)
    DEFAULT_OPERATOR_REGISTRY.register::<services::Http>(services::HTTP_SCHEME);
    DEFAULT_OPERATOR_REGISTRY.register::<services::Http>("https");

    let mut endpoint = Endpoint::builder();

    {
        let factory = OperatorFactory::Custom(Box::new(LambdaOperatorFactory::new(
            OperatorFactory::Chain(vec![
                OperatorFactory::Profiles(config.profiles.clone()),
                OperatorFactory::Default,
            ]),
            |o| o.layer(LoggingLayer::default()),
        )));

        if let Some(store_url) = config.store.uri {
            let operator = factory.from_uri(store_url.as_str())?;
            let service = scoped::ServiceImpl::new(operator);

            endpoint = endpoint.bind_with_options(service.serve(), config.restate.service.into())
        } else {
            let service = dynamic::ServiceImpl::new(factory);

            endpoint = endpoint.bind_with_options(service.serve(), config.restate.service.into())
        }
    }

    {
        let factory = OperatorFactory::Custom(Box::new(LambdaOperatorFactory::new(
            OperatorFactory::Chain(vec![
                OperatorFactory::Profiles(config.profiles.clone()),
                OperatorFactory::Default,
            ]),
            |o| o.layer(LoggingLayer::default()),
        )));

        endpoint = endpoint.bind(extra::ServiceImpl::new(factory).serve());
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
                Some("toml") => figment.merge(Toml::file(&path)),
                Some("json") => figment.merge(Json::file(&path)),
                Some("yaml") | Some("yml") => figment.merge(Yaml::file(&path)),
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
