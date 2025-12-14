use std::collections::HashMap;
use std::str::FromStr;

use opendal::{
    DEFAULT_OPERATOR_REGISTRY, Error, ErrorKind, Operator, OperatorRegistry, Result, Scheme,
};
use url::Url;

#[derive(Default)]
pub enum OperatorFactory {
    #[default]
    Default,
    Registry(OperatorRegistry),
    Profiles(HashMap<String, HashMap<String, String>>),
    Chain(Vec<OperatorFactory>),
    Custom(Box<dyn CustomOperatorFactory>),
}

impl OperatorFactory {
    pub fn from_uri(&self, uri: &str) -> Result<Operator> {
        match self {
            OperatorFactory::Default => DEFAULT_OPERATOR_REGISTRY.load(uri),
            OperatorFactory::Registry(registry) => registry.load(uri),
            OperatorFactory::Profiles(profiles) => {
                let url: Url = uri.parse().map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "Failed to parse URI")
                        .with_context("uri", uri)
                        .with_context("error", e)
                })?;

                let profile_name = url.scheme();

                let profile = profiles.get(profile_name).ok_or_else(|| {
                    Error::new(ErrorKind::Unsupported, "Profile not found")
                        .with_context("profile_name", profile_name)
                })?;

                let scheme = profile.get("type").ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "Missing 'type' in profile")
                        .with_context("profile", profile_name)
                })?;

                let scheme = Scheme::from_str(scheme)?;

                Operator::via_iter(scheme, profile.clone())
            }
            OperatorFactory::Chain(factories) => {
                for factory in factories {
                    match factory.from_uri(uri) {
                        Ok(op) => return Ok(op),
                        Err(e) if e.kind() == ErrorKind::Unsupported => continue,
                        Err(e) => return Err(e),
                    }
                }

                Err(Error::new(ErrorKind::Unsupported, "Unsupported URI").with_context("uri", uri))
            }
            OperatorFactory::Custom(r) => r.load(uri),
        }
    }
}

pub trait CustomOperatorFactory: Send + Sync {
    fn load(&self, uri: &str) -> Result<Operator>;
}

pub struct LambdaOperatorFactory<F> {
    inner: OperatorFactory,
    r#fn: F,
}

impl<F> LambdaOperatorFactory<F>
where
    F: Fn(Operator) -> Operator + Send + Sync,
{
    pub fn new(inner: OperatorFactory, r#fn: F) -> Self {
        Self { inner, r#fn }
    }
}

impl<F> CustomOperatorFactory for LambdaOperatorFactory<F>
where
    F: Fn(Operator) -> Operator + Send + Sync,
{
    fn load(&self, uri: &str) -> Result<Operator> {
        let op = self.inner.from_uri(uri)?;

        Ok((self.r#fn)(op))
    }
}
