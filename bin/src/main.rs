mod config;

use figment::{Figment, providers::Env};
use opendal::DEFAULT_OPERATOR_REGISTRY;
use opendal::layers::LoggingLayer;
use opendal::services;
use restate_opendal::LambdaOperatorFactory;
use restate_opendal::OperatorFactory;
use restate_opendal::{OpendalExtra, OpendalExtraImpl};
use restate_opendal::{
    dynamic, dynamic::Opendal as DynamicOpendal, scoped, scoped::Opendal as ScopedOpendal,
};
use restate_sdk::{endpoint::Endpoint, http_server::HttpServer};

use crate::config::Settings;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings: Settings = Figment::new()
        .merge(Env::raw().split("__"))
        .extract()
        .unwrap();

    // Get port from environment variable or use default
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9080);

    let bind_addr = format!("0.0.0.0:{}", port);

    // Register HTTP scheme (for some reason these are not registered by default)
    DEFAULT_OPERATOR_REGISTRY.register::<services::Http>(services::HTTP_SCHEME);
    DEFAULT_OPERATOR_REGISTRY.register::<services::Http>("https");

    let mut endpoint = Endpoint::builder();

    {
        let factory = OperatorFactory::Custom(Box::new(LambdaOperatorFactory::new(
            OperatorFactory::Default,
            |o| o.layer(LoggingLayer::default()),
        )));

        if let Some(store_url) = settings.store.uri {
            let operator = factory.from_uri(store_url.as_str()).unwrap();
            let service = scoped::OpendalImpl::new(operator);

            endpoint = endpoint.bind_with_options(service.serve(), settings.restate.service.into())
        } else {
            let service = dynamic::OpendalImpl::new(factory);

            endpoint = endpoint.bind_with_options(service.serve(), settings.restate.service.into())
        }
    }

    {
        let operator_factory = OperatorFactory::Custom(Box::new(LambdaOperatorFactory::new(
            OperatorFactory::Default,
            |o| o.layer(LoggingLayer::default()),
        )));

        endpoint = endpoint.bind(OpendalExtraImpl::new(operator_factory).serve());
    }

    // Create and start the HTTP server
    HttpServer::new(endpoint.build())
        .listen_and_serve(bind_addr.parse().unwrap())
        .await;
}
