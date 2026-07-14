//! Bot 本体。ライフサイクル (アカウント解決 → 接続 → dispatch loop → 終了)
//! を担う。WebSocket の再接続・keepalive は notecli 側の責務。

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use notecli::api::MisskeyClient;
use notecli::db::Database;
use notecli::event_bus::EventBus;
use notecli::models::{Account, NormalizedNote, NormalizedUser, TimelineType};
use notecli::streaming::StreamingManager;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::context::{BotAccount, BotHandle, Ctx};
use crate::error::{NotebotError, Result};
use crate::event::{parse_event, BotEvent, ChannelEmitter, SeenCache};
use crate::gate::SendGate;
use crate::router::parse_command;
use crate::scheduler::{spawn_jobs, Job, ScheduleHandler};
use crate::store::Store;

type Handler = Arc<dyn Fn(Ctx) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;
type ReactionHandler = Arc<
    dyn Fn(Ctx, ReactionEvent) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync,
>;
type FollowHandler = Arc<
    dyn Fn(BotHandle, NormalizedUser) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

/// 購読するタイムライン。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timeline {
    Home,
    Local,
    Social,
    Global,
}

impl Timeline {
    fn as_str(self) -> &'static str {
        match self {
            Timeline::Home => "home",
            Timeline::Local => "local",
            Timeline::Social => "social",
            Timeline::Global => "global",
        }
    }
}

/// 自分のノートに付いたリアクションの情報。
#[derive(Debug, Clone)]
pub struct ReactionEvent {
    pub reaction: String,
    pub user: Option<NormalizedUser>,
}

/// 再接続時の重複・mention/notification 二重発火を吸収する LRU 容量。
const SEEN_CACHE_CAPACITY: usize = 1024;
/// 最後に処理した mention の note id (catch-up 用)。
const LAST_MENTION_KEY: &str = "notebot.last_mention_id";
/// catch-up 1 ページの取得件数と最大ページ数。
const CATCHUP_PAGE_SIZE: i64 = 100;
const CATCHUP_MAX_PAGES: usize = 10;

pub struct BotBuilder {
    account_spec: Option<String>,
    commands: std::collections::HashMap<String, Handler>,
    on_mention: Option<Handler>,
    on_note: Vec<(Timeline, Handler)>,
    on_reaction: Option<ReactionHandler>,
    on_follow: Option<FollowHandler>,
    schedules: Vec<(String, ScheduleHandler)>,
    ignore_bots: bool,
}

impl BotBuilder {
    /// 使用するアカウント (`@user@host` / アカウントID / username)。
    /// 未指定なら notecli の先頭アカウント。環境変数 `NOTEBOT_ACCOUNT` が
    /// あればそちらが優先される (デプロイ用オーバーライド)。
    pub fn account(mut self, spec: impl Into<String>) -> Self {
        self.account_spec = Some(spec.into());
        self
    }

    /// メンションコマンド (`@bot <name> [引数...]`) のハンドラ。
    /// name は大文字小文字を区別しない。引数は `ctx.args()` で取れる。
    pub fn command<F, Fut>(mut self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(Ctx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.commands.insert(
            name.into().to_lowercase(),
            Arc::new(move |ctx| Box::pin(f(ctx))),
        );
        self
    }

    /// どのコマンドにも一致しないメンションのハンドラ。
    pub fn on_mention<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Ctx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_mention = Some(Arc::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    /// タイムラインに流れてきたノートのハンドラ。複数タイムラインを
    /// それぞれ別ハンドラで購読できる。自分のノートと bot のノートは
    /// フィルタされる (mention と同じ規則)。
    pub fn on_note<F, Fut>(mut self, timeline: Timeline, f: F) -> Self
    where
        F: Fn(Ctx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_note
            .push((timeline, Arc::new(move |ctx| Box::pin(f(ctx)))));
        self
    }

    /// 自分のノートにリアクションが付いたときのハンドラ。
    /// `ctx.note()` はリアクションが付いたノート。
    pub fn on_reaction<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Ctx, ReactionEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_reaction = Some(Arc::new(move |ctx, ev| Box::pin(f(ctx, ev))));
        self
    }

    /// フォローされたときのハンドラ (follow back など)。
    pub fn on_follow<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(BotHandle, NormalizedUser) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_follow = Some(Arc::new(move |bot, user| Box::pin(f(bot, user))));
        self
    }

    /// cron スケジュールでジョブを実行する (`"0 9 * * *"` = 毎日 9:00、
    /// ローカルタイムゾーン)。パターンの検証は `build()` で行う。
    pub fn schedule<F, Fut>(mut self, pattern: impl Into<String>, f: F) -> Self
    where
        F: Fn(BotHandle) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.schedules
            .push((pattern.into(), Arc::new(move |bot| Box::pin(f(bot)))));
        self
    }

    /// `isBot` ユーザーからのメンションを無視するか (デフォルト true)。
    /// bot 同士の無限ループ防止。false にする場合は自前で対策すること。
    pub fn ignore_bots(mut self, ignore: bool) -> Self {
        self.ignore_bots = ignore;
        self
    }

    pub fn build(self) -> Result<Bot> {
        let jobs = self
            .schedules
            .into_iter()
            .map(|(pattern, handler)| {
                let cron = croner::Cron::new(&pattern)
                    .with_seconds_optional()
                    .parse()
                    .map_err(|e| {
                        NotebotError::Config(format!("invalid cron pattern {pattern:?}: {e}"))
                    })?;
                Ok(Job {
                    cron,
                    source: pattern,
                    handler,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Bot {
            account_spec: self.account_spec,
            commands: self.commands,
            on_mention: self.on_mention,
            on_note: self.on_note,
            on_reaction: self.on_reaction,
            on_follow: self.on_follow,
            jobs,
            ignore_bots: self.ignore_bots,
        })
    }
}

pub struct Bot {
    account_spec: Option<String>,
    commands: std::collections::HashMap<String, Handler>,
    on_mention: Option<Handler>,
    on_note: Vec<(Timeline, Handler)>,
    on_reaction: Option<ReactionHandler>,
    on_follow: Option<FollowHandler>,
    jobs: Vec<Job>,
    ignore_bots: bool,
}

impl Bot {
    pub fn builder() -> BotBuilder {
        BotBuilder {
            account_spec: None,
            commands: std::collections::HashMap::new(),
            on_mention: None,
            on_note: Vec::new(),
            on_reaction: None,
            on_follow: None,
            schedules: Vec::new(),
            ignore_bots: true,
        }
    }

    /// bot を起動し、SIGINT/SIGTERM まで動き続ける。
    pub async fn run(mut self) -> Result<()> {
        let db = Arc::new(Database::open(&notecli_db_path())?);
        let client = Arc::new(MisskeyClient::new()?);
        let creds = self.resolve_credentials(&db, &client).await?;
        tracing::info!(account = %format!("@{}@{}", creds.username, creds.host), "starting bot");

        let store = Arc::new(Store::open(store_path(&creds.account_id))?);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let manager = StreamingManager::new(
            Arc::new(ChannelEmitter::new(tx.clone())),
            Arc::new(EventBus::new()),
            db.clone(),
        );
        manager
            .connect(&creds.account_id, &creds.host, &creds.token)
            .await?;
        manager.subscribe_main(&creds.account_id).await?;

        // on_note のタイムラインを購読し、subscription_id → handler を記録
        // (再接続時の replay は notecli が subscription 情報から行う)
        let mut note_routes: std::collections::HashMap<String, Handler> =
            std::collections::HashMap::new();
        for (timeline, handler) in &self.on_note {
            let sub_id = manager
                .subscribe_timeline(
                    &creds.account_id,
                    TimelineType::new(timeline.as_str()),
                    None,
                )
                .await?;
            tracing::info!(timeline = timeline.as_str(), "subscribed timeline");
            note_routes.insert(sub_id, handler.clone());
        }

        let account_id = creds.account_id.clone();
        let handle = BotHandle {
            client,
            account: Arc::new(BotAccount {
                id: creds.account_id,
                host: creds.host,
                token: creds.token,
                user_id: creds.user_id,
            }),
            gate: Arc::new(SendGate::new()),
            store,
        };
        let mut seen = SeenCache::new(SEEN_CACHE_CAPACITY);
        let jobs = spawn_jobs(std::mem::take(&mut self.jobs), &handle);

        loop {
            tokio::select! {
                _ = shutdown_signal() => {
                    tracing::info!("shutting down");
                    break;
                }
                recv = rx.recv() => {
                    let Some((name, payload)) = recv else { break };
                    self.handle_event(&name, payload, &handle, &tx, &mut seen, &note_routes);
                }
            }
        }
        for job in &jobs {
            job.abort();
        }
        manager.disconnect(&account_id).await;
        Ok(())
    }

    /// 認証情報の解決。優先順:
    /// 1. `NOTEBOT_TOKEN` (または `NOTEBOT_TOKEN_FILE`) + `NOTEBOT_HOST` —
    ///    トークンを DB に書かない直接注入。コンテナ運用の既定経路
    /// 2. notecli のアカウント (keychain / DB) — ローカル運用
    async fn resolve_credentials(
        &self,
        db: &Database,
        client: &MisskeyClient,
    ) -> Result<Credentials> {
        let env_token = read_env_token(
            std::env::var("NOTEBOT_TOKEN").ok(),
            std::env::var("NOTEBOT_TOKEN_FILE").ok(),
        )?;
        if let Some(token) = env_token {
            let host = std::env::var("NOTEBOT_HOST")
                .ok()
                .filter(|h| !h.is_empty())
                .ok_or_else(|| {
                    NotebotError::Config("NOTEBOT_TOKEN is set but NOTEBOT_HOST is missing".into())
                })?;
            // `i` で自分自身を取得 — トークン検証を兼ねる。user_id は
            // 自己応答ループ防止に必須
            let me = client
                .request(&host, &token, "i", serde_json::json!({}))
                .await?;
            let user_id = me
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| NotebotError::UnexpectedResponse("i: no id field".into()))?
                .to_string();
            let username = me
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("(unknown)")
                .to_string();
            return Ok(Credentials {
                account_id: format!("env:{user_id}@{host}"),
                host,
                token,
                user_id,
                username,
            });
        }

        let spec = std::env::var("NOTEBOT_ACCOUNT")
            .ok()
            .or_else(|| self.account_spec.clone());
        let account = resolve_account(db, spec.as_deref())?;
        let (host, token) = notecli::get_credentials(db, &account.id)?;
        Ok(Credentials {
            account_id: account.id.clone(),
            host,
            token,
            user_id: account.user_id.clone(),
            username: account.username.clone(),
        })
    }

    fn handle_event(
        &self,
        name: &str,
        payload: Value,
        handle: &BotHandle,
        tx: &mpsc::UnboundedSender<(String, Value)>,
        seen: &mut SeenCache,
        note_routes: &std::collections::HashMap<String, Handler>,
    ) {
        let Some(event) = parse_event(name, &payload) else {
            return;
        };
        match event {
            BotEvent::Status { state } => {
                tracing::info!(state, "stream status");
                if state == "connected" {
                    // 切断中に来たメンションを補完 (初回接続時は前回起動分)
                    spawn_catchup(handle.clone(), tx.clone());
                }
            }
            BotEvent::Mention(note) => {
                if !seen.insert(&format!("mention:{}", note.id)) {
                    return;
                }
                // 無視するメンションでも last id は進める —
                // catch-up で同じノートを永遠に再取得しないため
                record_last_mention(handle.store(), &note.id);
                if !should_handle(&note.user, &handle.account.user_id, self.ignore_bots) {
                    return;
                }
                let Some((handler, args)) = self.route(&note) else {
                    return;
                };
                spawn_handler(
                    "mention",
                    handler,
                    Ctx {
                        bot: handle.clone(),
                        note: *note,
                        args,
                    },
                );
            }
            BotEvent::Note {
                subscription_id,
                note,
            } => {
                // 同じノートが複数タイムラインに流れ得るため sub 単位で dedup
                if !seen.insert(&format!("note:{subscription_id}:{}", note.id)) {
                    return;
                }
                if !should_handle(&note.user, &handle.account.user_id, self.ignore_bots) {
                    return;
                }
                let Some(handler) = note_routes.get(&subscription_id) else {
                    return;
                };
                spawn_handler(
                    "note",
                    handler.clone(),
                    Ctx {
                        bot: handle.clone(),
                        note: *note,
                        args: Vec::new(),
                    },
                );
            }
            BotEvent::Notification(n) => {
                if !seen.insert(&format!("notif:{}", n.id)) {
                    return;
                }
                if let Some(user) = &n.user {
                    if !should_handle(user, &handle.account.user_id, self.ignore_bots) {
                        return;
                    }
                }
                match n.notification_type.as_str() {
                    "reaction" => {
                        let Some(handler) = &self.on_reaction else {
                            return;
                        };
                        // リアクション通知は対象ノートを含む
                        let Some(note) = n.note else { return };
                        let ev = ReactionEvent {
                            reaction: n.reaction.unwrap_or_default(),
                            user: n.user,
                        };
                        let handler = handler.clone();
                        let ctx = Ctx {
                            bot: handle.clone(),
                            note,
                            args: Vec::new(),
                        };
                        tokio::spawn(async move {
                            if let Err(e) = handler(ctx, ev).await {
                                tracing::error!(error = %e, "reaction handler failed");
                            }
                        });
                    }
                    "follow" => {
                        let Some(handler) = &self.on_follow else {
                            return;
                        };
                        let Some(user) = n.user else { return };
                        let handler = handler.clone();
                        let bot = handle.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handler(bot, user).await {
                                tracing::error!(error = %e, "follow handler failed");
                            }
                        });
                    }
                    _ => {}
                }
            }
        }
    }

    /// コマンドに一致すればそのハンドラと引数、しなければ on_mention。
    fn route(&self, note: &NormalizedNote) -> Option<(Handler, Vec<String>)> {
        if let Some((cmd, args)) = note.text.as_deref().and_then(parse_command) {
            if let Some(handler) = self.commands.get(&cmd) {
                return Some((handler.clone(), args));
            }
        }
        self.on_mention.as_ref().map(|h| (h.clone(), Vec::new()))
    }
}

struct Credentials {
    account_id: String,
    host: String,
    token: String,
    user_id: String,
    username: String,
}

/// 最後に処理した mention の note id を進める。Misskey の note id は
/// 時系列ソート可能な固定長文字列なので文字列比較で新旧を判定できる。
fn record_last_mention(store: &Store, id: &str) {
    let prev: Option<String> = store.get(LAST_MENTION_KEY).unwrap_or_default();
    if prev.as_deref().is_none_or(|p| id > p) {
        if let Err(e) = store.set(LAST_MENTION_KEY, &id) {
            tracing::warn!(error = %e, "failed to persist last mention id");
        }
    }
}

/// 切断中に取りこぼしたメンションを API で補完取得し、通常のイベント
/// パイプラインに `stream-mention` として再注入する (dedup・フィルタ・
/// ルーティングをそのまま通る)。last id が無い初回起動は履歴を replay
/// しない。
fn spawn_catchup(handle: BotHandle, tx: mpsc::UnboundedSender<(String, Value)>) {
    tokio::spawn(async move {
        let mut since_id = match handle.store().get::<String>(LAST_MENTION_KEY) {
            Ok(Some(id)) => id,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = %e, "catch-up: failed to read last mention id");
                return;
            }
        };
        for page in 0..CATCHUP_MAX_PAGES {
            let notes = match handle
                .client
                .get_mentions(
                    &handle.account.host,
                    &handle.account.token,
                    &handle.account.id,
                    CATCHUP_PAGE_SIZE,
                    Some(&since_id),
                    None,
                    None,
                )
                .await
            {
                Ok(notes) => notes,
                Err(e) => {
                    tracing::warn!(error = %e, "catch-up: fetch failed");
                    return;
                }
            };
            if notes.is_empty() {
                return;
            }
            let count = notes.len();
            let Some(max_id) = notes.iter().map(|n| n.id.clone()).max() else {
                return;
            };
            tracing::info!(count, page, "catch-up: replaying missed mentions");
            for note in notes {
                let payload = serde_json::json!({ "note": note });
                let _ = tx.send(("stream-mention".to_string(), payload));
            }
            if (count as i64) < CATCHUP_PAGE_SIZE {
                return;
            }
            since_id = max_id;
        }
        tracing::warn!(
            max = CATCHUP_MAX_PAGES * CATCHUP_PAGE_SIZE as usize,
            "catch-up: page limit reached; older mentions were skipped"
        );
    });
}

/// `NOTEBOT_TOKEN` / `NOTEBOT_TOKEN_FILE` からトークンを読む。
/// 前者が優先。どちらも無ければ None (notecli アカウントへフォールバック)。
fn read_env_token(token: Option<String>, token_file: Option<String>) -> Result<Option<String>> {
    if let Some(t) = token {
        let t = t.trim();
        if !t.is_empty() {
            return Ok(Some(t.to_string()));
        }
    }
    if let Some(path) = token_file.filter(|p| !p.is_empty()) {
        let t = std::fs::read_to_string(&path).map_err(|e| {
            NotebotError::Config(format!("failed to read NOTEBOT_TOKEN_FILE ({path}): {e}"))
        })?;
        let t = t.trim();
        if t.is_empty() {
            return Err(NotebotError::Config(format!(
                "NOTEBOT_TOKEN_FILE ({path}) is empty"
            )));
        }
        return Ok(Some(t.to_string()));
    }
    Ok(None)
}

/// 自己応答ループ防止 (解除不可) と isBot 無視 (オプトアウト可)。
fn should_handle(user: &NormalizedUser, self_user_id: &str, ignore_bots: bool) -> bool {
    if user.id == self_user_id {
        return false;
    }
    if ignore_bots && user.is_bot {
        tracing::debug!(user = user.username, "ignoring event from bot user");
        return false;
    }
    true
}

/// ハンドラを task 分離で実行: Err/panic で bot 本体を落とさない。
fn spawn_handler(kind: &'static str, handler: Handler, ctx: Ctx) {
    let note_id = ctx.note.id.clone();
    tokio::spawn(async move {
        if let Err(e) = handler(ctx).await {
            tracing::error!(kind, note_id, error = %e, "handler failed");
        }
    });
}

/// notecli と同じ解決規則: `@user@host` → アカウントID → username。
/// (notecli の resolve_account は private のため同等品を持つ)
fn resolve_account(db: &Database, spec: Option<&str>) -> Result<Account> {
    let accounts = db.load_accounts()?;
    if accounts.is_empty() {
        return Err(NotebotError::AccountNotFound(
            "no accounts found — run `notecli login <HOST>` first".to_string(),
        ));
    }
    let Some(spec) = spec else {
        return Ok(accounts.into_iter().next().unwrap());
    };
    if let Some(rest) = spec.strip_prefix('@') {
        if let Some((user, host)) = rest.split_once('@') {
            return accounts
                .into_iter()
                .find(|a| a.username.eq_ignore_ascii_case(user) && a.host.contains(host))
                .ok_or_else(|| NotebotError::AccountNotFound(spec.to_string()));
        }
    }
    if let Some(account) = db.get_account(spec)? {
        return Ok(account);
    }
    accounts
        .into_iter()
        .find(|a| a.username.eq_ignore_ascii_case(spec))
        .ok_or_else(|| NotebotError::AccountNotFound(spec.to_string()))
}

fn data_base_dir() -> PathBuf {
    dirs::data_dir().unwrap_or_else(|| {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(home).join(".local/share")
    })
}

fn notecli_db_path() -> PathBuf {
    data_base_dir().join("notecli").join("notecli.db")
}

/// `{data_dir}/notebot/{account_id}/store.json`。account_id には
/// `env:{user_id}@{host}` 形式が来るためファイル名に安全な文字へ丸める。
fn store_path(account_id: &str) -> PathBuf {
    let safe: String = account_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    data_base_dir()
        .join("notebot")
        .join(safe)
        .join("store.json")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(user_id: &str, is_bot: bool) -> NormalizedNote {
        serde_json::from_value(serde_json::json!({
            "id": "n1",
            "_accountId": "acc1",
            "_serverHost": "misskey.example",
            "createdAt": "2026-07-15T00:00:00.000Z",
            "text": "@bot hello",
            "user": { "id": user_id, "username": "someone", "isBot": is_bot },
            "visibility": "public",
            "renoteCount": 0,
            "repliesCount": 0
        }))
        .unwrap()
    }

    #[test]
    fn own_note_is_always_ignored() {
        assert!(!should_handle(&note("me", false).user, "me", false));
        assert!(!should_handle(&note("me", false).user, "me", true));
    }

    #[test]
    fn bot_note_is_ignored_by_default() {
        assert!(!should_handle(&note("other", true).user, "me", true));
        assert!(should_handle(&note("other", true).user, "me", false));
    }

    #[test]
    fn normal_mention_is_handled() {
        assert!(should_handle(&note("other", false).user, "me", true));
    }

    fn seed_account(db: &Database, id: &str, username: &str, host: &str) {
        db.upsert_account(&Account {
            id: id.to_string(),
            host: host.to_string(),
            token: "t".to_string(),
            user_id: format!("u-{id}"),
            username: username.to_string(),
            display_name: None,
            avatar_url: None,
            software: "misskey".to_string(),
        })
        .unwrap();
    }

    #[test]
    fn resolves_accounts_like_notecli() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("test.db")).unwrap();
        seed_account(&db, "a1", "alice", "misskey.example");
        seed_account(&db, "a2", "bob", "other.example");

        assert_eq!(resolve_account(&db, None).unwrap().id, "a1");
        assert_eq!(
            resolve_account(&db, Some("@bob@other.example")).unwrap().id,
            "a2"
        );
        assert_eq!(resolve_account(&db, Some("a2")).unwrap().id, "a2");
        assert_eq!(resolve_account(&db, Some("BOB")).unwrap().id, "a2");
        assert!(matches!(
            resolve_account(&db, Some("@nobody@nowhere")),
            Err(NotebotError::AccountNotFound(_))
        ));
    }

    #[test]
    fn env_token_prefers_direct_value() {
        let got = read_env_token(Some(" tok ".into()), Some("/nonexistent".into())).unwrap();
        assert_eq!(got.as_deref(), Some("tok"));
    }

    #[test]
    fn env_token_reads_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "filetok\n").unwrap();
        let got = read_env_token(None, Some(path.to_string_lossy().into_owned())).unwrap();
        assert_eq!(got.as_deref(), Some("filetok"));
    }

    #[test]
    fn env_token_absent_falls_back() {
        assert!(read_env_token(None, None).unwrap().is_none());
        // 空文字は未設定扱い
        assert!(read_env_token(Some("".into()), None).unwrap().is_none());
    }

    #[test]
    fn env_token_missing_file_is_config_error() {
        assert!(matches!(
            read_env_token(None, Some("/nonexistent/token".into())),
            Err(NotebotError::Config(_))
        ));
    }

    #[test]
    fn build_rejects_invalid_cron_pattern() {
        let result = Bot::builder()
            .schedule("not a cron", |_| async { Ok(()) })
            .build();
        assert!(matches!(result, Err(NotebotError::Config(_))));
        assert!(Bot::builder()
            .schedule("0 9 * * *", |_| async { Ok(()) })
            .build()
            .is_ok());
    }

    #[test]
    fn last_mention_id_only_moves_forward() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("store.json")).unwrap();
        record_last_mention(&store, "aaa2");
        record_last_mention(&store, "aaa1"); // 古い id では戻らない
        assert_eq!(
            store.get::<String>(LAST_MENTION_KEY).unwrap().as_deref(),
            Some("aaa2")
        );
        record_last_mention(&store, "aaa3");
        assert_eq!(
            store.get::<String>(LAST_MENTION_KEY).unwrap().as_deref(),
            Some("aaa3")
        );
    }

    #[test]
    fn store_path_sanitizes_account_id() {
        let path = store_path("env:u1@misskey.example");
        let dir_name = path
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy();
        assert_eq!(dir_name, "env-u1-misskey.example");
    }

    #[test]
    fn empty_db_gives_guidance() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("test.db")).unwrap();
        let err = resolve_account(&db, None).unwrap_err();
        assert!(err.to_string().contains("notecli login"));
    }
}
