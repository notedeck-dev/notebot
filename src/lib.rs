//! notebot — notecli をコアにした Misskey bot フレームワーク。
//!
//! ```no_run
//! use notebot::Bot;
//!
//! #[tokio::main]
//! async fn main() -> notebot::Result<()> {
//!     Bot::builder()
//!         .account("@mybot@misskey.example")
//!         .on_mention(|ctx| async move {
//!             ctx.reply("pong").await?;
//!             Ok(())
//!         })
//!         .build()?
//!         .run()
//!         .await
//! }
//! ```

pub mod bot;
pub mod context;
pub mod error;
pub mod event;
mod gate;
mod router;
mod scheduler;
pub mod store;

pub use bot::{Bot, BotBuilder, ReactionEvent, Timeline};
pub use context::{BotHandle, Ctx};
pub use error::{NotebotError, Result};
pub use event::BotEvent;
pub use store::Store;

/// エスケープハッチ: notecli のモデル・クライアントを直接使う場合に。
pub use notecli;
