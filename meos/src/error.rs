use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("expected a different meos type")]
    WrongTemporalType,

    #[error("meos internal error {0}")]
    MeosError(i32),

    #[error("ffi string conversion error {0}")]
    FfiStringError(String),
}
