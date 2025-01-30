pub enum EtlFramework {
    Polars,
    DuckDB,
    DataFusion,
}
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Clone, Debug, Serialize)]
pub enum Error {
    LoginFail,
    DatabaseError(String),
    DataExist(String),
    DataNotAvaliable(String),
    TokenError(String),
    DecodeError(String),
    StringError(String),
    UserUnauthorized(String),
    SmtpProcessingError(String),
    UserNotVerified(String),
    UploadProcessingError(String),
    CloudAuthError(String),
    InvalidUserType(String),
}

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        write!(fmt, "{self:?}")
    }
}

impl std::error::Error for Error {}

pub trait EtlStage {
    async fn extraction(&mut self) -> Result<()>;
    async fn transformation(&mut self) -> Result<()>;
    async fn write(&mut self) -> Result<()>;
}


impl EtlFramework for EtlStage {
    async fn extraction(&mut self) -> Result<()> {
        // Implement extraction logic here
       match self {
        EtlFramework::Polars => unimplemented!(),
        EtlFramework::DuckDB => unimplemented!(),
        EtlFramework::DataFusion => unimplemented!(),
       }
    }
}