//! フォローバック + リアクションカウンタ + タイムライン監視のデモ。
//!
//! - フォローされたら follow back して挨拶をリプライ相当のノートで投稿
//! - 自分のノートにリアクションが付いたら累計を Store に記録
//! - ローカル TL で "notebot" を含むノートに 👀

use notebot::{Bot, Timeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "notebot=info,notecli=warn".into()),
        )
        .init();

    Bot::builder()
        .on_follow(|bot, user| async move {
            bot.follow(&user.id).await?;
            bot.post(&format!(
                "@{} フォローありがとうございます！",
                user.username
            ))
            .await?;
            Ok(())
        })
        .on_reaction(|ctx, ev| async move {
            let count: u64 = ctx.store().get("reaction_count")?.unwrap_or(0) + 1;
            ctx.store().set("reaction_count", &count)?;
            tracing::info!(reaction = ev.reaction, count, "reaction received");
            Ok(())
        })
        .on_note(Timeline::Local, |ctx| async move {
            let mentions_us = ctx
                .note()
                .text
                .as_deref()
                .is_some_and(|t| t.contains("notebot"));
            if mentions_us {
                ctx.react("👀").await?;
            }
            Ok(())
        })
        .build()?
        .run()
        .await?;
    Ok(())
}
