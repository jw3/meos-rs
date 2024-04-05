use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("meos internal error {0}")]
    MeosError(i32),
}
