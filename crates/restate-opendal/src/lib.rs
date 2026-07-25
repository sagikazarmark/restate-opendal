mod service;

mod service_scoped;
pub mod scoped {
    pub use super::service_scoped::*;
}

mod service_dynamic;
pub mod dynamic {
    pub use super::service_dynamic::*;
}

mod service_extra;
pub mod extra {
    pub use super::service_extra::*;
}

mod error;

#[cfg(test)]
mod tests {
    use opendal_util::DefaultOperatorFactory;
    use restate_sdk::{discovery::ServiceType as RestateServiceType, service::Discoverable};

    use super::{dynamic, extra, scoped};

    #[test]
    fn discovers_opendal_apis() {
        let scoped = <scoped::ServiceImpl as Discoverable>::discover();
        assert_service(&scoped, "OpenDAL", &["list", "presignRead", "presignStat"]);

        let dynamic = <dynamic::ServiceImpl<DefaultOperatorFactory> as Discoverable>::discover();
        assert_service(&dynamic, "OpenDAL", &["list", "presignRead", "presignStat"]);

        let extra = <extra::ServiceImpl<DefaultOperatorFactory> as Discoverable>::discover();
        assert_service(&extra, "OpenDALExtra", &["copy"]);
    }

    #[test]
    fn serializes_copy_options_as_camel_case() {
        let request = extra::CopyRequest {
            source: "memory:///source".parse().unwrap(),
            destination: "memory:///destination".parse().unwrap(),
            options: Some(opendal_util::CopyOptions {
                recursive: true,
                disable_glob: true,
            }),
        };

        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(
            value["options"],
            serde_json::json!({"recursive": true, "disableGlob": true}),
        );

        let request: extra::CopyRequest = serde_json::from_value(value).unwrap();
        let options = request.options.unwrap();
        assert!(options.recursive);
        assert!(options.disable_glob);
    }

    #[test]
    fn maps_read_content_length_hint() {
        let options: opendal::options::ReadOptions = scoped::ReadOptions {
            content_length_hint: Some(1024),
            ..Default::default()
        }
        .into();

        assert_eq!(options.content_length_hint, Some(1024));
    }

    fn assert_service(
        service: &restate_sdk::discovery::Service,
        name: &str,
        expected_handlers: &[&str],
    ) {
        assert_eq!(service.name.as_str(), name);
        assert_eq!(service.ty, RestateServiceType::Service);

        let mut handlers: Vec<_> = service
            .handlers
            .iter()
            .map(|handler| handler.name.as_str())
            .collect();
        handlers.sort_unstable();

        let mut expected_handlers = expected_handlers.to_vec();
        expected_handlers.sort_unstable();
        assert_eq!(handlers, expected_handlers);
    }
}
