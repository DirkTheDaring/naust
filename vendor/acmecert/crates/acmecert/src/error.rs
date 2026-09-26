use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum CliError {
    Message(String),
    Core(acmecert_core::api::AppError),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Message(msg) => f.write_str(msg),
            CliError::Core(err) => fmt::Display::fmt(err, f),
        }
    }
}

impl Error for CliError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            CliError::Message(_) => None,
            CliError::Core(err) => Some(err),
        }
    }
}

impl From<String> for CliError {
    fn from(value: String) -> Self {
        CliError::Message(value)
    }
}

impl From<acmecert_core::api::AppError> for CliError {
    fn from(value: acmecert_core::api::AppError) -> Self {
        CliError::Core(value)
    }
}
