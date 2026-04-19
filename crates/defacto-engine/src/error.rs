use thiserror::Error;

#[derive(Error, Debug)]
pub enum DefactoError {
    #[error("Definition error: {0}")]
    Definition(String),

    #[error("Ingest error: {0}")]
    Ingest(String),

    #[error("Build error: {0}")]
    Build(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Identity error: {0}")]
    Identity(String),
}
