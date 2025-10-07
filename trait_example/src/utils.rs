#[macro_export] // Implement From<T> for common error conversions
macro_rules! impl_from_error {
    ($($type:ty => $variant:ident),* $(,)?) => {
        $(impl From<$type> for Error {
            fn from(error: $type) -> Self {
                tracing::error!("{}", error);
                Error::$variant(error.to_string())
            }
        })*
    };
}
