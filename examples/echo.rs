//! メンションされた内容をそのまま返信する echo bot。
//!
//! ```sh
//! notecli login misskey.example          # 事前にアカウント登録
//! NOTEBOT_ACCOUNT=@mybot@misskey.example cargo run --example echo
//! ```

use notebot::Bot;

/// 文頭のメンション (`@bot` / `@bot@host`) を取り除く。
fn strip_leading_mentions(text: &str) -> &str {
    let mut rest = text.trim_start();
    while let Some(after) = rest.strip_prefix('@') {
        let end = after
            .find(|c: char| c.is_whitespace())
            .unwrap_or(after.len());
        rest = after[end..].trim_start();
    }
    rest
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "notebot=info,notecli=warn".into()),
        )
        .init();

    Bot::builder()
        .on_mention(|ctx| async move {
            let text = ctx.note().text.clone().unwrap_or_default();
            let body = strip_leading_mentions(&text);
            if body.is_empty() {
                ctx.react("👋").await?;
            } else {
                ctx.reply(body).await?;
            }
            Ok(())
        })
        .build()?
        .run()
        .await?;
    Ok(())
}
