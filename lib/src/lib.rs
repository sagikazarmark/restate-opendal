mod service;

mod service_scoped;
pub mod scoped {
    pub use super::service::*;
    pub use super::service_scoped::*;
}

mod service_dynamic;
pub mod dynamic {
    pub use super::service::*;
    pub use super::service_dynamic::*;
}

mod service_extra;
pub use service_extra::*;

mod error;

mod factory;
pub use factory::*;
