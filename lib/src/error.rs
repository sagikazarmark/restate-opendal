use opendal_util::to_restate_error;
use restate_sdk::errors::HandlerError;

#[derive(Debug)]
pub struct Error(HandlerError);

impl From<opendal::Error> for Error {
    fn from(err: opendal::Error) -> Self {
        Self(to_restate_error(err))
    }
}

impl From<Error> for HandlerError {
    fn from(err: Error) -> HandlerError {
        err.0
    }
}

#[cfg(test)]
mod tests {
    use opendal::ErrorKind;

    use super::*;

    #[test]
    fn maps_permanent_opendal_errors_to_terminal_errors() {
        let err = opendal::Error::new(ErrorKind::NotFound, "not found").set_permanent();
        let handler_error: HandlerError = Error::from(err).into();
        let source: &(dyn std::error::Error + Send + Sync) = handler_error.as_ref();

        assert!(source.to_string().contains("Terminal error [404]"));
        assert!(source.to_string().contains("not found"));
        assert!(
            source
                .source()
                .unwrap()
                .downcast_ref::<opendal::Error>()
                .is_none()
        );
    }

    #[test]
    fn keeps_transient_opendal_errors_retryable() {
        let err = opendal::Error::new(ErrorKind::Unexpected, "try again").set_temporary();
        let handler_error: HandlerError = Error::from(err).into();
        let source: &(dyn std::error::Error + Send + Sync) = handler_error.as_ref();

        assert!(source.to_string().starts_with("Retryable error:"));
        assert!(source.to_string().contains("try again"));
        assert!(
            source
                .source()
                .unwrap()
                .downcast_ref::<opendal::Error>()
                .is_some()
        );
    }
}
