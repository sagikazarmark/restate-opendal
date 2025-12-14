use opendal::{DEFAULT_OPERATOR_REGISTRY, Operator, OperatorRegistry, Result};

#[derive(Default)]
pub enum OperatorFactory {
    #[default]
    Default,
    Registry(OperatorRegistry),
    Custom(Box<dyn CustomOperatorFactory>),
}

impl OperatorFactory {
    pub fn from_uri(&self, uri: &str) -> Result<Operator> {
        match self {
            OperatorFactory::Default => DEFAULT_OPERATOR_REGISTRY.load(uri),
            OperatorFactory::Registry(r) => r.load(uri),
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
