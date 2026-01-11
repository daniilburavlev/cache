use core::fmt;
use std::{num::TryFromIntError, string::FromUtf8Error};

const INVALID_FRAME: &str = "protocol error: invalid frame format";

#[derive(Debug)]
pub enum CacheError {
    EndOfStream,
    Incomplete,
    Other(String),
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::EndOfStream => "unexpected end of stream".fmt(f),
            CacheError::Incomplete => "stream ended early".fmt(f),
            CacheError::Other(err) => err.fmt(f),
        }
    }
}

impl From<std::io::Error> for CacheError {
    fn from(value: std::io::Error) -> Self {
        CacheError::Other(value.to_string())
    }
}

impl From<String> for CacheError {
    fn from(value: String) -> Self {
        CacheError::Other(value)
    }
}

impl From<&str> for CacheError {
    fn from(value: &str) -> Self {
        CacheError::Other(value.to_string())
    }
}

impl From<TryFromIntError> for CacheError {
    fn from(_: TryFromIntError) -> Self {
        INVALID_FRAME.into()
    }
}

impl From<FromUtf8Error> for CacheError {
    fn from(_: FromUtf8Error) -> Self {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for CacheError {}
