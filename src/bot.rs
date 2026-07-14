//! Bot 本体。ライフサイクル (アカウント解決 → 接続 → dispatch loop → 終了)
//! を担う。WebSocket の再接続・keepalive は notecli 側の責務。

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use notecli::api::MisskeyClient;
use notecli::db::Database;
use notecli::event_bus::EventBus;
use notecli::models::{Account, NormalizedNote};
use notecli::streaming::StreamingManager;
use serde_json::Value;
use tokio::sync::mpsc;

use crate::context::{BotAccount, Ctx};
use crate::error::{NotebotError, Result};
use crate::event::{parse_event, BotEvent, ChannelEmitter};

type Handler = Arc<dyn Fn(Ctx) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

pub struct BotBuilder {
    account_spec: Option<String>,
    on_mention: Option<Handler>,
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

    /// メンションを受けたときのハンドラ。
    pub fn on_mention<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(Ctx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.on_mention = Some(Arc::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    /// `isBot` ユーザーからのメンションを無視するか (デフォルト true)。
    /// bot 同士の無限ループ防止。false にする場合は自前で対策すること。
    pub fn ignore_bots(mut self, ignore: bool) -> Self {
        self.ignore_bots = ignore;
        self
    }

    pub fn build(self) -> Result<Bot> {
        Ok(Bot {
            account_spec: self.account_spec,
            on_mention: self.on_mention,
            ignore_bots: self.ignore_bots,
        })
    }
}

pub struct Bot {
    account_spec: Option<String>,
    on_mention: Option<Handler>,
    ignore_bots: bool,
}

impl Bot {
    pub fn builder() -> BotBuilder {
        BotBuilder {
            account_spec: None,
            on_mention: None,
            ignore_bots: true,
        }
    }

    /// bot を起動し、SIGINT/SIGTERM まで動き続ける。
    pub async fn run(self) -> Result<()> {
        let db = Arc::new(Database::open(&notecli_db_path())?);
        let client = Arc::new(MisskeyClient::new()?);
        let creds = self.resolve_credentials(&db, &client).await?;
        tracing::info!(account = %format!("@{}@{}", creds.username, creds.host), "starting bot");

        let (tx, mut rx) = mpsc::unbounded_channel();
        let manager = StreamingManager::new(
            Arc::new(ChannelEmitter::new(tx)),
            Arc::new(EventBus::new()),
            db.clone(),
        );
        manager.connect(&creds.account_id, &creds.host, &creds.token).await?;
        manager.subscribe_main(&creds.account_id).await?;

        let account_id = creds.account_id.clone();
        let bot_account = Arc::new(BotAccount {
            id: creds.account_id,
            host: creds.host,
            token: creds.token,
            user_id: creds.user_id,
        });

        loop {
            tokio::select! {
                _ = shutdown_signal() => {
                    tracing::info!("shutting down");
                    break;
                }
                recv = rx.recv() => {
                    let Some((name, payload)) = recv else { break };
                    self.handle_event(&name, payload, &client, &bot_account);
                }
            }
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
            let host = std::env::var("NOTEBOT_HOST").ok().filter(|h| !h.is_empty()).ok_or_else(|| {
                NotebotError::Config("NOTEBOT_TOKEN is set but NOTEBOT_HOST is missing".into())
            })?;
            // `i` で自分自身を取得 — トークン検証を兼ねる。user_id は
            // 自己応答ループ防止に必須
            let me = client.request(&host, &token, "i", serde_json::json!({})).await?;
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

        let spec = std::env::var("NOTEBOT_ACCOUNT").ok().or_else(|| self.account_spec.clone());
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
        client: &Arc<MisskeyClient>,
        account: &Arc<BotAccount>,
    ) {
        let Some(event) = parse_event(name, &payload) else {
            return;
        };
        match event {
            BotEvent::Status { state } => tracing::info!(state, "stream status"),
            BotEvent::Mention(note) => {
                if !should_handle(&note, &account.user_id, self.ignore_bots) {
                    return;
                }
                let Some(handler) = &self.on_mention else {
                    return;
                };
                let ctx = Ctx {
                    client: client.clone(),
                    account: account.clone(),
                    note: *note,
                };
                let note_id = ctx.note.id.clone();
                let handler = handler.clone();
                // ハンドラは task 分離: Err/panic で bot 本体を落とさない
                tokio::spawn(async move {
                    if let Err(e) = handler(ctx).await {
                        tracing::error!(note_id, error = %e, "mention handler failed");
                    }
                });
            }
        }
    }
}

struct Credentials {
    account_id: String,
    host: String,
    token: String,
    user_id: String,
    username: String,
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
fn should_handle(note: &NormalizedNote, self_user_id: &str, ignore_bots: bool) -> bool {
    if note.user.id == self_user_id {
        return false;
    }
    if ignore_bots && note.user.is_bot {
        tracing::debug!(note_id = note.id, "ignoring mention from bot user");
        return false;
    }
    true
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

fn notecli_db_path() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".local/share")
        })
        .join("notecli")
        .join("notecli.db")
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
        assert!(!should_handle(&note("me", false), "me", false));
        assert!(!should_handle(&note("me", false), "me", true));
    }

    #[test]
    fn bot_note_is_ignored_by_default() {
        assert!(!should_handle(&note("other", true), "me", true));
        assert!(should_handle(&note("other", true), "me", false));
    }

    #[test]
    fn normal_mention_is_handled() {
        assert!(should_handle(&note("other", false), "me", true));
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
        assert_eq!(resolve_account(&db, Some("@bob@other.example")).unwrap().id, "a2");
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
    fn empty_db_gives_guidance() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("test.db")).unwrap();
        let err = resolve_account(&db, None).unwrap_err();
        assert!(err.to_string().contains("notecli login"));
    }
}
