//! ハンドラに渡すコンテキスト。MisskeyClient はステートレス (host/token を
//! 毎回引数に取る) ため、Ctx がそれらを束ねて省略可能にする。

use std::sync::Arc;

use notecli::api::MisskeyClient;
use notecli::models::{CreateNoteParams, NormalizedNote, RawNote};

use crate::error::{NotebotError, Result};

#[derive(Debug)]
pub(crate) struct BotAccount {
    pub id: String,
    pub host: String,
    pub token: String,
    pub user_id: String,
}

pub struct Ctx {
    pub(crate) client: Arc<MisskeyClient>,
    pub(crate) account: Arc<BotAccount>,
    pub(crate) note: NormalizedNote,
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
            .client
            .create_note(&self.account.host, &self.account.token, &self.account.id, params)
            .await?)
    }

    async fn reply_specified(&self, text: &str) -> Result<NormalizedNote> {
        // 宛先 = 元ノートの visibleUserIds + 投稿者、自分自身は除く
        let mut ids: Vec<String> = self
            .note
            .visible_user_ids
            .iter()
            .filter(|id| **id != self.account.user_id)
            .cloned()
            .collect();
        if self.note.user.id != self.account.user_id && !ids.contains(&self.note.user.id) {
            ids.push(self.note.user.id.clone());
        }
        let body = serde_json::json!({
            "text": text,
            "replyId": self.note.id,
            "visibility": "specified",
            "visibleUserIds": ids,
        });
        let data = self
            .client
            .request(&self.account.host, &self.account.token, "notes/create", body)
            .await?;
        let created = data
            .get("createdNote")
            .cloned()
            .ok_or_else(|| NotebotError::UnexpectedResponse("notes/create: no createdNote".into()))?;
        let raw: RawNote = serde_json::from_value(created)
            .map_err(notecli::error::NoteDeckError::from)?;
        Ok(raw.normalize(&self.account.id, &self.account.host))
    }

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
            .client
            .create_note(&self.account.host, &self.account.token, &self.account.id, params)
            .await?)
    }

    /// 発火元ノートにリアクションする。
    pub async fn react(&self, reaction: &str) -> Result<()> {
        self.client
            .create_reaction(&self.account.host, &self.account.token, &self.note.id, reaction)
            .await?;
        Ok(())
    }

    /// エスケープハッチ: notecli の生クライアントと認証情報。
    pub fn client(&self) -> (&MisskeyClient, &str, &str) {
        (&self.client, &self.account.host, &self.account.token)
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
