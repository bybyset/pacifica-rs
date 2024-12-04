use anyerror::AnyError;
use thiserror::Error;

#[derive(PartialEq, Eq)]
#[derive(Debug, Error)]
pub enum OptionError {
    #[error("ParseError: {source} while parsing ({args:?})")]
    ParseError { source: AnyError, args: Vec<String> },

    #[error("{reason} when parsing '{parse_str}'")]
    InvalidStr { reason: String, parse_str: String },
}
