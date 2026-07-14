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

M3 まで実装済み:

- M1: `on_mention` + `Ctx::reply/post/react`、自己応答ループ防止、`isBot`
  無視、visibility 継承 (public→home 丸め)、DM (specified) 返信対応
- M2: コマンドルーター (`.command("dice", ...)` + `ctx.args()`)、
  送信直列化 + `RATE_LIMIT_EXCEEDED` 指数バックオフ再送、
  イベント dedup (LRU 1024)
- M3: cron スケジューラ (`.schedule("0 9 * * *", ...)`)、KV ストア
  (`ctx.store()`)、再接続 catch-up (切断中のメンションを API で補完 —
  WebSocket は切断中のイベントを replay しないため)
- M4: `on_note(Timeline::Local, ...)` / `on_reaction` / `on_follow`
  (+ `BotHandle::follow` で follow back)、GitHub Actions CI。
  on_chat は notecli 側の対応待ち (ARCHITECTURE.md 参照)

設計の詳細は [ARCHITECTURE.md](ARCHITECTURE.md) を参照。

```sh
cargo run --example echo      # メンションをオウム返しする bot
cargo run --example dice      # ダイスロール (@bot dice 6 3 / @bot ping)
cargo run --example greeter   # フォローバック + リアクション集計 + TL 監視
```

## Docker Compose での運用

トークンは `.env` から環境変数で注入する。**DB やイメージには書かれない。**

1. bot アカウントで Misskey Web にログインし、設定 → API →
   アクセストークンの発行（権限は「ノートを作成・削除する」「リアクションを
   追加・削除する」程度に絞れる）
2. リポジトリ直下に `.env` を作成（.gitignore / .dockerignore 済み）:

   ```sh
   NOTEBOT_HOST=misskey.example
   NOTEBOT_TOKEN=<発行したトークン>
   ```

3. 起動:

   ```sh
   docker compose up -d --build
   docker compose logs -f bot
   ```

- docker secrets 等でファイル渡しする場合は `NOTEBOT_TOKEN` の代わりに
  `NOTEBOT_TOKEN_FILE=/run/secrets/notebot_token`
- 別の bot を動かす場合: `docker compose build --build-arg EXAMPLE=<name>`
  （`examples/<name>.rs` をビルドする）
- volume (`notebot-data`) にはノートキャッシュのみが残る

ローカル開発では従来どおり `notecli login` したアカウント
（OS キーチェーン保管）が使える。`NOTEBOT_TOKEN` が設定されていれば
そちらが優先される。

## License

MIT
