pub type Result<T> = core::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub enum Error {
    Polars(String),
    Io(String),
    Reqwest(String),
    HeaderName(String),
    HeaderValue(String),
    SerdeJson(String),
    Sqlx(String),
    JoinError(String),
}

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        write!(fmt, "{self:?}")
    }
}

impl std::error::Error for Error {}

crate::impl_from_error!(
    polars::prelude::PolarsError => Polars,
    std::io::Error => Io,
    reqwest::Error => Reqwest,
    reqwest::header::InvalidHeaderValue => HeaderValue,
    reqwest::header::InvalidHeaderName => HeaderName,
    serde_json::Error => SerdeJson,
    sqlx::Error => Sqlx,
    tokio::task::JoinError => JoinError,
);
