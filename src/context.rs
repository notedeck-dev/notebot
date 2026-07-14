//! ハンドラに渡すコンテキスト。MisskeyClient はステートレス (host/token を
//! 毎回引数に取る) ため、Ctx がそれらを束ねて省略可能にする。

use std::sync::Arc;

use notecli::api::MisskeyClient;
use notecli::models::{CreateNoteParams, NormalizedNote, RawNote};

use crate::error::{NotebotError, Result};
use crate::gate::SendGate;
use crate::store::Store;

#[derive(Debug)]
pub(crate) struct BotAccount {
    pub id: String,
    pub host: String,
    pub token: String,
    pub user_id: String,
}

/// note 文脈なしで bot として操作できるハンドル。schedule ハンドラに渡され、
/// Ctx も内部でこれを持つ。
#[derive(Clone)]
pub struct BotHandle {
    pub(crate) client: Arc<MisskeyClient>,
    pub(crate) account: Arc<BotAccount>,
    pub(crate) gate: Arc<SendGate>,
    pub(crate) store: Arc<Store>,
}

impl BotHandle {
    /// 新規ノートを投稿する (visibility はアカウントのデフォルト)。
    pub async fn post(&self, text: &str) -> Result<NormalizedNote> {
        let params = CreateNoteParams {
            text: Some(text.to_string()),
            cw: None,
            visibility: None,
            local_only: None,
            mode_flags: None,
            reply_id: None,
            renote_id: None,
            file_ids: None,
            poll: None,
            scheduled_at: None,
        };
        Ok(self
            .gate
            .send(|| {
                self.client.create_note(
                    &self.account.host,
                    &self.account.token,
                    &self.account.id,
                    params.clone(),
                )
            })
            .await?)
    }

    /// ユーザーをフォローする (follow back 用)。
    pub async fn follow(&self, user_id: &str) -> Result<()> {
        self.gate
            .send(|| {
                self.client
                    .follow_user(&self.account.host, &self.account.token, user_id)
            })
            .await?;
        Ok(())
    }

    /// bot 用 KV ストア。
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// エスケープハッチ: notecli の生クライアントと認証情報。
    pub fn client(&self) -> (&MisskeyClient, &str, &str) {
        (&self.client, &self.account.host, &self.account.token)
    }
}

pub struct Ctx {
    pub(crate) bot: BotHandle,
    pub(crate) note: NormalizedNote,
    pub(crate) args: Vec<String>,
}

/// 返信の visibility は元ノートを継承するが、public は home に丸める
/// (bot の返信で public TL を汚さない)。
fn reply_visibility(original: &str) -> &str {
    match original {
        "public" => "home",
        other => other,
    }
}

impl Ctx {
    /// 発火元ノート。
    pub fn note(&self) -> &NormalizedNote {
        &self.note
    }

    /// コマンド引数 (`@bot dice 6 2` なら `["6", "2"]`)。
    /// `on_mention` ハンドラでは常に空。
    pub fn args(&self) -> &[String] {
        &self.args
    }

    /// note 文脈なしの bot 操作 (post / store / client)。
    pub fn bot(&self) -> &BotHandle {
        &self.bot
    }

    /// bot 用 KV ストア。
    pub fn store(&self) -> &Store {
        self.bot.store()
    }

    /// 発火元ノートへ返信する。visibility は継承 (public→home 丸め)、
    /// local_only も継承する。
    pub async fn reply(&self, text: &str) -> Result<NormalizedNote> {
        let visibility = reply_visibility(&self.note.visibility);
        if visibility == "specified" {
            // CreateNoteParams に visibleUserIds が無い (notecli v0.4.0) ため
            // 生 request で回避。上流にフィールド追加を提案予定。
            return self.reply_specified(text).await;
        }
        let params = CreateNoteParams {
            text: Some(text.to_string()),
            cw: None,
            visibility: Some(visibility.to_string()),
            local_only: self.note.local_only.then_some(true),
            mode_flags: None,
            reply_id: Some(self.note.id.clone()),
            renote_id: None,
            file_ids: None,
            poll: None,
            scheduled_at: None,
        };
        Ok(self
            .bot
            .gate
            .send(|| {
                self.bot.client.create_note(
                    &self.bot.account.host,
                    &self.bot.account.token,
                    &self.bot.account.id,
                    params.clone(),
                )
            })
            .await?)
    }

    async fn reply_specified(&self, text: &str) -> Result<NormalizedNote> {
        let account = &self.bot.account;
        // 宛先 = 元ノートの visibleUserIds + 投稿者、自分自身は除く
        let mut ids: Vec<String> = self
            .note
            .visible_user_ids
            .iter()
            .filter(|id| **id != account.user_id)
            .cloned()
            .collect();
        if self.note.user.id != account.user_id && !ids.contains(&self.note.user.id) {
            ids.push(self.note.user.id.clone());
        }
        let body = serde_json::json!({
            "text": text,
            "replyId": self.note.id,
            "visibility": "specified",
            "visibleUserIds": ids,
        });
        let data = self
            .bot
            .gate
            .send(|| {
                self.bot
                    .client
                    .request(&account.host, &account.token, "notes/create", body.clone())
            })
            .await?;
        let created = data.get("createdNote").cloned().ok_or_else(|| {
            NotebotError::UnexpectedResponse("notes/create: no createdNote".into())
        })?;
        let raw: RawNote =
            serde_json::from_value(created).map_err(notecli::error::NoteDeckError::from)?;
        Ok(raw.normalize(&account.id, &account.host))
    }

    /// 新規ノートを投稿する (visibility はアカウントのデフォルト)。
    pub async fn post(&self, text: &str) -> Result<NormalizedNote> {
        self.bot.post(text).await
    }

    /// 発火元ノートにリアクションする。
    pub async fn react(&self, reaction: &str) -> Result<()> {
        self.bot
            .gate
            .send(|| {
                self.bot.client.create_reaction(
                    &self.bot.account.host,
                    &self.bot.account.token,
                    &self.note.id,
                    reaction,
                )
            })
            .await?;
        Ok(())
    }

    /// エスケープハッチ: notecli の生クライアントと認証情報。
    pub fn client(&self) -> (&MisskeyClient, &str, &str) {
        self.bot.client()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_is_rounded_to_home() {
        assert_eq!(reply_visibility("public"), "home");
    }

    #[test]
    fn other_visibilities_are_inherited() {
        assert_eq!(reply_visibility("home"), "home");
        assert_eq!(reply_visibility("followers"), "followers");
        assert_eq!(reply_visibility("specified"), "specified");
    }
}
