# notebot アーキテクチャ

notecli をコアに据えた Misskey bot フレームワーク。
notecli が「Misskey をヘッドレスで操作する能力」を提供し、notebot は
**イベント駆動の bot ランタイム**（ハンドラ登録・コマンドルーティング・
スケジューラ・安全装置）だけを提供する。Misskey API・WebSocket・認証は
一切再実装しない。

## レイヤー構成

```
bot 実装 (examples/, ユーザーのクレート)
   │  Bot::builder().command("ping", ...).run()
   ▼
notebot (このリポジトリ)
   ├── bot.rs        Bot / BotBuilder — ライフサイクルと実行ループ
   ├── event.rs      ChannelEmitter + BotEvent — notecli イベントの型付け
   ├── router.rs     メンションコマンドの解析・ディスパッチ
   ├── context.rs    Ctx — reply/react/post 等の便利 API
   ├── scheduler.rs  cron タスク
   ├── store.rs      bot 用 KV ストア (notebot.db)
   └── error.rs      NotebotError
   ▼
notecli (git dependency)
   ├── api::MisskeyClient        100+ エンドポイントの HTTP ラッパー
   ├── streaming::StreamingManager  WS 接続・自動再接続・polling fallback
   ├── db::Database + keychain   アカウント・トークン管理 (notecli login)
   └── models::Normalized*       正規化済みモデル
```

## 1. イベント取り込み — `FrontendEmitter` を実装する

notecli の `streaming::FrontendEmitter` は公式のイベント配信拡張ポイント
（notedeck も同じ経路で消費）。notebot は mpsc チャンネルに流すだけの
emitter を実装する。

```rust
pub struct ChannelEmitter(mpsc::UnboundedSender<(String, Value)>);

impl FrontendEmitter for ChannelEmitter {
    fn emit(&self, event: &str, payload: Value) {
        let _ = self.0.send((event.to_string(), payload));
    }
}
```

受信側の dispatch loop が notecli の emit イベント名を型付き enum に変換する:

| notecli イベント名 | BotEvent | 発火元チャンネル |
|---|---|---|
| `stream-mention` | `Mention(NormalizedNote)` | main |
| `stream-note` | `Note { subscription_id, note }` | timeline/antenna/channel |
| `stream-notification` | `Notification(StreamNotificationEvent)` | main (reaction/follow/reply/renote 等を含む) |
| `stream-chat-message` | `ChatMessage(..)` | chat |
| `stream-main-event` | `MainEvent { kind, body }` | main (followed 等) |
| `stream-status` | `Status { state }` | 接続状態 (ログ用途) |

デシリアライズ失敗は `tracing::warn` してスキップ（notecli 側の
イベント追加で bot が落ちないこと）。

## 2. ライフサイクル — `Bot::run()`

```
run()
 ├─ Database::open (notecli の data_dir/notecli.db を共有)
 ├─ resolve_account(account 指定) → get_credentials() で (host, token)
 ├─ StreamingManager::new(Arc<ChannelEmitter>, event_bus, db)
 ├─ connect(account_id, host, token)
 ├─ subscribe_main(account_id)                    … 常時 (mention/notification)
 ├─ on_note 登録があれば subscribe_timeline(..)    … 必要時のみ
 ├─ on_chat 登録があれば subscribe_chat_*(..)
 ├─ dispatch loop (mpsc 受信 → フィルタ → ハンドラ spawn)
 └─ SIGINT/SIGTERM で graceful shutdown
```

- 再接続・keepalive・polling fallback はすべて notecli 側の責務。notebot は
  `stream-status` をログするだけ。
- 各ハンドラは `tokio::spawn` で分離実行。ハンドラの `Err` は
  `tracing::error` でログし bot 本体は落とさない。panic も task 境界で吸収。

## 3. ハンドラ API

```rust
Bot::builder()
    .account("@mybot@misskey.example")        // notecli login 済みアカウント
    // メンションコマンド: "@mybot ping" → ping ハンドラ
    .command("ping", |ctx: Ctx| async move {
        ctx.reply("pong").await?;
        Ok(())
    })
    .command("dice", |ctx| async move {
        let n: u32 = ctx.args().first().and_then(|s| s.parse().ok()).unwrap_or(6);
        ctx.reply(&format!("🎲 {}", rand::random_range(1..=n))).await?;
        Ok(())
    })
    .on_mention(|ctx| async move { ... })      // どのコマンドにも一致しないメンション
    .on_note(Timeline::Local, |ctx| async move { ... })
    .on_reaction(|ctx, ev| async move { ... })
    .on_follow(|ctx, user| async move { ctx.follow_back().await })
    .on_chat(|ctx, msg| async move { ... })    // DM (chat)
    .schedule("0 9 * * *", |bot: BotHandle| async move {
        bot.post("おはよう").await?;
        Ok(())
    })
    .build()?
    .run()
    .await
```

ハンドラの実体は `Box<dyn Fn(Ctx) -> BoxFuture<'static, Result<()>> + Send + Sync>`。
内部ではすべてのハンドラを単一の `Handler` trait に正規化して保持する。
これにより将来、状態を持つ bot 向けの Module trait（藍スタイル）を
closure API を壊さずに追加できる。

### コマンドルーティング (router.rs)

1. `stream-mention` の note text の文頭メンション連続を**すべて**読み飛ばす
   （自分宛だけでなく、リプライチェーンで自動付与される他ユーザー宛も。
   mention イベント自体が「bot がメンションされた」ことを保証している）
2. 最初の通常トークンをコマンド名として lookup（大文字小文字無視）
3. 残りを空白分割して `ctx.args()` に
4. 一致なし・本文なし → `on_mention` フォールバック

MFM の完全パースはしない。プレーンテキストとしての先頭トークン一致のみ
（必要になったら mfm パーサ導入を検討）。

## 4. Ctx — ハンドラに渡すコンテキスト

`MisskeyClient` は host/token を毎回引数に取るステートレス設計なので、
Ctx がそれらを束ねて省略可能にする。

```rust
pub struct Ctx {
    client: Arc<MisskeyClient>,
    account: AccountInfo,          // account_id, host, token, 自分の user_id
    note: Option<NormalizedNote>,  // 発火元ノート (mention/note 時)
    args: Vec<String>,             // コマンド引数
    store: Store,
}

impl Ctx {
    /// 発火元ノートへ返信。visibility は元ノートを継承
    /// (public は home に丸める — bot の返信で public TL を汚さない)
    pub async fn reply(&self, text: &str) -> Result<NormalizedNote>;
    pub async fn reply_with(&self, params: CreateNoteParams) -> Result<NormalizedNote>;
    pub async fn react(&self, reaction: &str) -> Result<()>;
    pub async fn post(&self, text: &str) -> Result<NormalizedNote>;
    pub async fn renote(&self) -> Result<()>;
    pub fn note(&self) -> Option<&NormalizedNote>;
    // 注意: 元ノートが specified (DM) の場合、返信にも visibleUserIds が
    // 必要だが CreateNoteParams に該当フィールドが無い (notecli v0.4.0
    // 時点で確認済み)。当面は client().request() の生呼び出しで回避し、
    // 上流 (notecli) へのフィールド追加を提案する。
    pub fn args(&self) -> &[String];
    pub fn store(&self) -> &Store;
    /// エスケープハッチ: notecli の生クライアント
    pub fn client(&self) -> (&MisskeyClient, &str /* host */, &str /* token */);
}
```

`schedule` ハンドラには note 文脈がないため、`post` 系のみ持つ
`BotHandle` を渡す。

## 5. 安全装置（フレームワークとして必須）

bot 特有の事故をデフォルトで防ぐ。すべて builder でオプトアウト可能。

| 装置 | デフォルト | 理由 |
|---|---|---|
| 自分のノートを無視 | 常時 on (解除不可) | 自己応答ループ防止 |
| `isBot` ユーザーを無視 | on | bot 同士の無限ループ防止 |
| 返信 visibility を継承 + public→home 丸め | on | TL 汚染防止 |
| 送信キューの直列化 + `RATE_LIMIT_EXCEEDED` 時の指数バックオフ再送 | on | サーバー負荷・凍結対策。Misskey のレート制限はエンドポイント毎なので固定間隔ではなくエラー駆動で制御する |
| イベント dedup (note id の LRU 1024 件) | on | mention は `stream-mention` と `stream-notification` の**両方で発火する**(streaming.rs で確認済み)ため必須。再接続時の重複にも効く |

運用上の注意: 同一アカウントで notebot を 2 プロセス起動すると二重応答に
なる。プロセス管理はデプロイ側の責務とし、v0.1 ではガードしない
(README に明記する)。

## 6. 状態管理 — Store

`{data_dir}/notebot/{account_id}/store.json`。notecli のデータディレクトリ
には間借りしない（bot の状態は bot の持ち物）。

**実装は JSON ファイル + atomic rename**（tmp に書いて rename）。SQLite に
しない理由: rusqlite (libsqlite3-sys) は Cargo の `links` 制約により
**依存グラフ全体で 1 バージョンしか共存できず**、notebot が直接依存すると
notecli の rusqlite バージョンと恒久的なロックステップを強いられる。
bot の状態（last-seen id、カウンタ程度）に SQLite は過剰でもある。
大きな状態が必要になったら pure-Rust の redb を検討する。

```rust
impl Store {
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>>;
    pub fn set<T: Serialize>(&self, key: &str, value: &T) -> Result<()>;
    pub fn delete(&self, key: &str) -> Result<()>;
}
```

bot ごとの複雑な永続化が必要なら利用者が自前の DB を持てばよい
（notebot は面倒を見ない）。

## 7. スケジューラ

`croner` で cron 式をパースし、tokio task で次回発火まで sleep する
素朴な実装。永続化しない（プロセス再起動で次周期から）。ジョブの
`Err`/panic はログのみ。

## 8. 認証と設定

認証情報の解決は 2 経路、優先順:

1. **環境変数直接注入** — `NOTEBOT_TOKEN`（または `NOTEBOT_TOKEN_FILE`、
   docker secrets 向け）+ `NOTEBOT_HOST`。トークンは Misskey Web の
   「設定 → API → アクセストークンの発行」で取得する。起動時に `i`
   エンドポイントで user_id を取得（トークン検証を兼ね、自己応答ループ
   防止にも必要）。**DB にはトークンもアカウント行も書かない**
   （`StreamingManager::get_host` は connect 時に渡した host をメモリから
   返すため、DB のアカウント行は不要 — 実装確認済み）。
   コンテナ運用の既定経路
2. **notecli のアカウント** — `notecli login <host>`（MiAuth → OS
   キーチェーン保存）済みアカウントを `resolve_account()` +
   `get_credentials()` で解決。`.account("@bot@host")` のコード指定、
   `NOTEBOT_ACCOUNT` 環境変数でオーバーライド可。ローカル運用向け
- notecli.db の共有について（実装確認済み）: notecli は
  `PRAGMA journal_mode=WAL` で開くため、CLI と notebot の並行利用は許容
  される。**read-only オープンは不可** — `StreamingManager` が受信ノートを
  `db.cache_note()` で書き込むため、通常の読み書きオープンで共有する。

### コンテナ運用 (Docker Compose)

- 認証は経路 1（`NOTEBOT_TOKEN` 直接注入）を使う。トークンは `.env`
  （.gitignore / .dockerignore 対象）から compose が環境変数として渡し、
  **DB・イメージ・volume には残らない**
- コンテナ内では OS キーチェーンがどのみち使えない（Docker のデフォルト
  seccomp が `keyctl`/`add_key` を塞ぐ。仮に通っても kernel keyring は
  非永続で、notecli の lazy migration が DB トークンをクリアして再起動で
  トークンを失う）。**Docker ビルドは `--no-default-features`**（notebot の
  `keyring` feature を落とす）で、keyring 依存ごとコンパイルしない
- イメージは example bot (`ARG EXAMPLE`, 既定 echo) を `/usr/local/bin/bot`
  として同梱。SIGTERM は `run()` が処理するため `docker stop` で
  graceful shutdown になる。volume (`/data`) はノートキャッシュのみ

## 9. エラー型

```rust
#[derive(thiserror::Error, Debug)]
pub enum NotebotError {
    #[error(transparent)]
    Core(#[from] notecli::error::NoteDeckError),
    #[error("account not found: {0}")]
    AccountNotFound(String),
    #[error("store error: {0}")]
    Store(#[from] rusqlite::Error),
    // ...
}
```

## 10. 依存関係

```toml
[dependencies]
notecli = { git = "https://github.com/hitalin/notecli.git", rev = "<pin>" }
tokio   = { version = "1", features = ["rt-multi-thread", "macros", "signal", "sync", "time"] }
serde / serde_json
thiserror = "2"
tracing / tracing-subscriber
croner    = "2"       # cron 式パース
```

rusqlite への直接依存は持たない（§6 の `links` 制約を参照）。SQLite は
notecli 経由でのみ触る。

- notecli は crates.io 未公開なので git 依存 + rev ピン。semver 未安定
  (v0.4.x) のため、更新は明示的に rev を進める。
- CI は notecli に倣い `--no-default-features`（keyring 無効）でも通す。
  ただし bot の実運用はトークン必須なので keyring はデフォルト有効。

## 11. テスト戦略

notecli と同じ流儀:

- `wiremock` で Misskey API をモック。`MisskeyClient::with_base_url` は
  `#[cfg(test)]` で notebot からは使えないが、notecli の `insecure` 機構
  （環境変数 `NOTECLI_INSECURE_HOSTS=127.0.0.1:PORT`、debug ビルド限定）で
  wiremock の http ホストに接続できる（実装確認済み）。env はプロセス共有
  なのでテストは serial 実行にする
- router のコマンド解析はユニットテストで網羅
  （`@bot ping`, `@bot@host ping 引数`, メンションなし、CW 付き等）
- dispatch loop は `ChannelEmitter` に直接イベントを注入して検証
  （WebSocket 不要でフレームワーク全体をテストできるのがこの設計の利点）
- `cargo clippy -- -D warnings`

## 12. マイルストーン

1. **M1 (echo bot)**: Bot/BotBuilder + ChannelEmitter + dispatch loop +
   `on_mention` + `Ctx::reply`。`examples/echo.rs` が実サーバーで動く
2. **M2 (安全な bot)**: command router + 安全装置一式（self/bot 無視、
   visibility 丸め、レートリミット、dedup）
3. **M3 (常駐 bot)**: scheduler + Store + graceful shutdown +
   **再接続 catch-up**（`stream-status: connected` を受けたら、Store に
   永続化した last-seen notification id から `get_notifications(since_id)`
   で切断中のメンションを補完取得する。WebSocket は切断中のイベントを
   replay しないため、これが無いと bot は取りこぼす）
4. **M4 (公開品質)**: on_note/on_reaction/on_follow、examples 拡充、
   README、CI。**on_chat は見送り** — notecli の `stream-chat-message` は
   per-peer の chat 購読 (`subscribe_chat_user`) からのみ発火し、bot は
   相手を事前に知れない。main チャンネルの `newChatMessage` は汎用
   `stream-main-event` に落ちるが payload 形状が未検証。chat 対応は
   notecli 側に「main チャンネル経由の chat メッセージ購読」を足してから
   (上流課題)。DM は specified ノート (mention 経由) で従来どおり扱える

## スコープ外 (v0.1 では作らない)

- 複数アカウント同時稼働（StreamingManager は対応済みなので将来容易）
- プラグインシステム・動的ロード
- 対話ステートマシン（会話の文脈追跡）
- Webhook / HTTP 入力（notecli daemon がすでにある）
- MFM の完全パース
