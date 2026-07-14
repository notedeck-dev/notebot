#[derive(Debug, thiserror::Error)]
pub enum NotebotError {
    #[error(transparent)]
    Core(#[from] notecli::error::NoteDeckError),
    #[error("account not found: {0}")]
    AccountNotFound(String),
    #[error("unexpected API response: {0}")]
    UnexpectedResponse(String),
}

pub type Result<T> = std::result::Result<T, NotebotError>;
