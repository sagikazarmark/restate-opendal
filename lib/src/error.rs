use opendal::ErrorKind;
use restate_sdk::errors::{HandlerError, TerminalError};

#[derive(Debug)]
pub struct Error(HandlerError);

impl From<opendal::Error> for Error {
    fn from(err: opendal::Error) -> Self {
        Error(classify_opendal_error(err))
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        let msg = err.to_string();
        if let Ok(opendal_err) = err.downcast::<opendal::Error>() {
            Error(classify_opendal_error(opendal_err))
        } else {
            Error(HandlerError::from(msg))
        }
    }
}

impl From<TerminalError> for Error {
    fn from(err: TerminalError) -> Self {
        Error(err.into())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error(err.into())
    }
}

fn classify_opendal_error(err: opendal::Error) -> HandlerError {
    if err.is_permanent() {
        let status_code = match err.kind() {
            ErrorKind::Unsupported => 501,
            ErrorKind::ConfigInvalid => 400,
            ErrorKind::NotFound => 404,
            ErrorKind::PermissionDenied => 403,
            ErrorKind::IsADirectory => 422,
            ErrorKind::NotADirectory => 422,
            ErrorKind::AlreadyExists => 409,
            _ => 500,
        };

        return TerminalError::new_with_code(status_code, err.to_string()).into();
    }

    err.into()
}

impl From<Error> for HandlerError {
    fn from(err: Error) -> HandlerError {
        err.0
    }
}

#[allow(dead_code)]
pub trait TerminalExt<T, E> {
    fn terminal(self) -> Result<T, HandlerError>;
}

impl<T, E> TerminalExt<T, E> for Result<T, E>
where
    E: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static,
{
    fn terminal(self) -> Result<T, HandlerError> {
        self.map_err(|err| TerminalError::new(err.to_string()).into())
    }
}

#[macro_export]
macro_rules! terminal {
    ($msg:literal $(,)?) => {
        return Err(restate_sdk::errors::TerminalError::new($msg).into())
    };
    ($err:expr $(,)?) => {
        return Err(restate_sdk::errors::TerminalError::new($err.to_string()).into())
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err(restate_sdk::errors::TerminalError::new(format!($fmt, $($arg)*)).into())
    };
}

#[allow(dead_code)]
pub trait OpendalResultExt<T> {
    fn into_handler_error(self) -> Result<T, HandlerError>;
}

impl<T> OpendalResultExt<T> for Result<T, opendal::Error> {
    fn into_handler_error(self) -> Result<T, HandlerError> {
        self.map_err(|err| {
            if err.is_permanent() {
                return TerminalError::new(err.to_string()).into();
            }

            err.into()
        })
    }
}
