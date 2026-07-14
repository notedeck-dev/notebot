//! notecli の `FrontendEmitter` イベントを型付き `BotEvent` に変換する。
//!
//! notecli の Stream*Event は Serialize のみ derive しているため、
//! ここでは emit された JSON payload から必要な部分だけを取り出す。

use notecli::models::NormalizedNote;
use notecli::streaming::FrontendEmitter;
use serde_json::Value;
use tokio::sync::mpsc;

/// StreamingManager が emit したイベントを mpsc に流すだけの emitter。
///
/// EventBus (broadcast) は受信遅延でイベントを落とすが、mpsc unbounded は
/// lossless — bot がメンションを取りこぼさないためにこちらを使う。
pub(crate) struct ChannelEmitter(mpsc::UnboundedSender<(String, Value)>);

impl ChannelEmitter {
    pub(crate) fn new(tx: mpsc::UnboundedSender<(String, Value)>) -> Self {
        Self(tx)
    }
}

impl FrontendEmitter for ChannelEmitter {
    fn emit(&self, event: &str, payload: Value) {
        // 受信側 drop 後の send 失敗は shutdown 中なので無視してよい
        let _ = self.0.send((event.to_string(), payload));
    }
}

/// notebot が扱うイベント。M1 では mention と接続状態のみ。
#[derive(Debug)]
pub enum BotEvent {
    Mention(Box<NormalizedNote>),
    Status { state: String },
}

/// emit されたイベント名 + payload を BotEvent に変換する。
/// 未知のイベントは None(無視)、デシリアライズ失敗は warn して None —
/// notecli 側のイベント追加・変更で bot が落ちないこと。
pub(crate) fn parse_event(name: &str, payload: &Value) -> Option<BotEvent> {
    match name {
        "stream-mention" => match serde_json::from_value(payload.get("note")?.clone()) {
            Ok(note) => Some(BotEvent::Mention(Box::new(note))),
            Err(e) => {
                tracing::warn!(error = %e, "failed to deserialize mention note");
                None
            }
        },
        "stream-status" => Some(BotEvent::Status {
            state: payload.get("state")?.as_str()?.to_string(),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mention_payload() -> Value {
        json!({
            "accountId": "acc1",
            "subscriptionId": "sub1",
            "note": {
                "id": "n1",
                "_accountId": "acc1",
                "_serverHost": "misskey.example",
                "createdAt": "2026-07-15T00:00:00.000Z",
                "text": "@bot hello",
                "user": { "id": "u1", "username": "alice" },
                "visibility": "public",
                "renoteCount": 0,
                "repliesCount": 0
            }
        })
    }

    #[test]
    fn parses_mention() {
        let Some(BotEvent::Mention(note)) = parse_event("stream-mention", &mention_payload())
        else {
            panic!("expected Mention");
        };
        assert_eq!(note.id, "n1");
        assert_eq!(note.user.username, "alice");
        assert!(!note.user.is_bot);
    }

    #[test]
    fn parses_status() {
        let payload = json!({ "accountId": "acc1", "state": "reconnecting" });
        let Some(BotEvent::Status { state }) = parse_event("stream-status", &payload) else {
            panic!("expected Status");
        };
        assert_eq!(state, "reconnecting");
    }

    #[test]
    fn unknown_event_is_ignored() {
        assert!(parse_event("stream-note-updated", &json!({})).is_none());
    }

    #[test]
    fn malformed_mention_is_ignored() {
        let payload = json!({ "note": { "id": 42 } });
        assert!(parse_event("stream-mention", &payload).is_none());
    }
}
