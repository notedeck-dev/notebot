#[derive(Debug, thiserror::Error)]
pub enum NotebotError {
    #[error(transparent)]
    Core(#[from] notecli::error::NoteDeckError),
    #[error("account not found: {0}")]
    AccountNotFound(String),
    #[error("unexpected API response: {0}")]
    UnexpectedResponse(String),
    #[error("configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, NotebotError>;
