# notebot

[notecli](https://github.com/hitalin/notecli) をコアにした Misskey bot フレームワーク。

Misskey API・WebSocket ストリーミング（自動再接続つき）・MiAuth 認証・
トークンのキーチェーン保管はすべて notecli が担い、notebot は
**ハンドラを書くだけで bot が動く**イベント駆動ランタイムを提供する。

## 使い方（設計目標）

```rust
use notebot::{Bot, Ctx};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Bot::builder()
        .account("@mybot@misskey.example")   // notecli login 済みアカウント
        .command("ping", |ctx: Ctx| async move {
            ctx.reply("pong").await?;
            Ok(())
        })
        .on_mention(|ctx| async move {
            ctx.react("👍").await?;
            Ok(())
        })
        .schedule("0 9 * * *", |bot| async move {
            bot.post("おはようございます").await?;
            Ok(())
        })
        .build()?
        .run()
        .await
}
```

セットアップは 2 ステップ:

```sh
notecli login misskey.example   # MiAuth でトークン取得 → OS キーチェーンへ
cargo run --example echo        # bot 起動
```

## 特徴

- **再実装ゼロ**: HTTP API / WebSocket / 認証 / 再接続 / polling fallback は
  notecli のライブラリ機能をそのまま利用
- **安全装置がデフォルト**: 自己応答ループ防止、bot 同士のループ防止
  (`isBot` 無視)、返信 visibility の継承（public→home 丸め）、
  送信レートリミット、再接続時の重複イベント除去
- **コマンドルーター**: `@bot ping 引数...` を宣言的にルーティング
- **cron スケジューラ**と **KV ストア**を同梱

## ステータス

M1 (echo bot が動く最小構成) 実装済み: `on_mention` + `Ctx::reply/post/react`、
自己応答ループ防止、`isBot` 無視、visibility 継承 (public→home 丸め)、
DM (specified) 返信対応。次は M2 (command router + 残りの安全装置)。
設計の詳細は [ARCHITECTURE.md](ARCHITECTURE.md) を参照。

```sh
cargo run --example echo   # メンションをオウム返しする bot
```

## License

MIT
