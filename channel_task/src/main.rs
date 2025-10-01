use clap::{command, Parser};
use serde::Serialize;
use tracing::error as log_error;

pub type Result<T> = core::result::Result<T, Error>;
#[derive(Clone, Debug, Serialize)]
pub enum Error {
    OsFileOperationFailed(String)
}

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::result::Result<(), core::fmt::Error> {
        write!(fmt, "{self:?}")
    }
}

impl std::error::Error for Error {}

macro_rules! impl_from_error {
    ($($type:ty => $variant:ident),* $(,)?) => {
        $(impl From<$type> for Error {
            fn from(error: $type) -> Self {
                log_error!("{}", error);
                Error::$variant(error.to_string())
            }
        })*
    };
}

impl_from_error!(
    std::io::Error => OsFileOperationFailed,
    walkdir::Error => OsFileOperationFailed,
);

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, help="path to the directory")]
    path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let path = &cli.path;
    let entries = walkdir::WalkDir::new(path);

    let files = entries.into_iter().filter_map(|path| {
        match path {
            Ok(entry) => {
                if entry.file_type().is_file() {
                    Some(entry)
                } else {
                    None
                }
            }
            Err(err) => {
                log_error!("WalkDir error: {}", err);
                None
            }
        }
    });

    for file in files {
        println!("{}", file.path().display());

        
    }

    Ok(())
} 
