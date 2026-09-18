// 中心服专用的回环 WS 通道，复用语音进程中的识别状态。
use super::*;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::time::{interval, timeout, MissedTickBehavior};
use warp::ws::{Message, WebSocket};

fn apply_command(state: &mut SharedState, value: Value) -> Result<(), String> {
    if value.get("type").and_then(Value::as_str) != Some("recognition") {
        return Err("unknown_command".into());
    }
    let mut body = value.get("data").cloned().ok_or("invalid_body")?;
    let hash = body.get("machine_hash").and_then(Value::as_str).ok_or("invalid_machine_hash")?;
    let token = state.issue_token(hash)?;
    let object = body.as_object_mut().ok_or("invalid_body")?;
    object.insert("auth_token".into(), Value::String(token));
    let request: RecognitionReportRequest = serde_json::from_value(body).map_err(|_| "invalid_presence")?;
    if request.server_address.len() > 128 || request.players.len() > 2 {
        return Err("invalid_presence".into());
    }
    let ip = value.get("ip").and_then(Value::as_str).and_then(|ip| ip.parse().ok());
    state.apply_report(Instant::now(), request, ip)?;
    Ok(())
}

fn snapshot(shared: &Arc<Mutex<SharedState>>) -> Option<String> {
    let mut state = shared.lock().ok()?;
    // IP 只用于内部鉴别，不包含在客户端推送中。
    let mut users = state.users(Instant::now(), None);
    for user in &mut users {
        user.last_ip = None;
    }
    Some(json!({"type":"users", "data":{"users":users}}).to_string())
}

async fn connection(socket: WebSocket, shared: Arc<Mutex<SharedState>>) {
    let mut changed = match shared.lock() {
        Ok(state) => state.realtime_changed.subscribe(),
        Err(_) => return,
    };
    let (mut sender, mut receiver) = socket.split();
    let mut tick = interval(Duration::from_secs(10));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let outgoing = tokio::select! {
            message = receiver.next() => {
                let Some(Ok(message)) = message else { break };
                if message.is_close() { break; }
                if !message.is_text() { continue; }
                let result = serde_json::from_slice::<Value>(message.as_bytes())
                    .map_err(|_| "invalid_json".to_string())
                    .and_then(|value| {
                        shared.lock().map_err(|_| "state_unavailable".to_string())
                            .and_then(|mut state| apply_command(&mut state, value))
                    });
                match result {
                    Ok(()) => continue,
                    Err(error) => Some(json!({"type":"error", "data":{"error":error}}).to_string()),
                }
            }
            result = changed.changed() => {
                if result.is_err() { break; }
                // 合并同一批上报，慢接收者始终取得最新快照。
                tokio::time::sleep(Duration::from_millis(100)).await;
                changed.borrow_and_update();
                snapshot(&shared)
            }
            _ = tick.tick() => snapshot(&shared),
        };
        if let Some(text) = outgoing {
            if !matches!(timeout(Duration::from_secs(5), sender.send(Message::text(text))).await, Ok(Ok(()))) {
                break;
            }
        }
    }
}

pub(super) fn routes(shared: Arc<Mutex<SharedState>>) -> impl Filter<Extract = (impl Reply,), Error = warp::Rejection> + Clone {
    warp::path("qm")
        .and(warp::path("realtime"))
        .and(warp::path::end())
        .and(warp::addr::remote())
        .and_then(|remote: Option<SocketAddr>| async move {
            if remote.is_some_and(|address| address.ip().is_loopback()) {
                Ok(())
            } else {
                Err(warp::reject::not_found())
            }
        })
        .untuple_one()
        .and(warp::ws())
        .and(with_shared_state(shared))
        .map(|ws: warp::ws::Ws, state| ws.max_message_size(64 * 1024).on_upgrade(move |socket| connection(socket, state)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_report_reuses_identity_and_announces_changes() {
        let mut state = SharedState::new("test".into(), Duration::from_secs(60));
        let changes = state.realtime_changed.subscribe();
        let hash = "a".repeat(64);
        apply_command(&mut state, json!({"type":"recognition", "ip":"192.0.2.1", "data":{
            "machine_hash":hash, "server_address":"one:8303", "players":[{"player_name":"玩家"}]
        }})).unwrap();
        assert!(changes.has_changed().unwrap());
        assert_eq!(state.users(Instant::now(), None)[0].qid, state.issue_token(&hash).unwrap());
        state.cleanup(Instant::now() + Duration::from_secs(61));
        assert!(state.users(Instant::now(), None).is_empty());
    }

    #[test]
    fn invalid_internal_command_does_not_change_presence() {
        let mut state = SharedState::new("test".into(), Duration::from_secs(60));
        assert!(apply_command(&mut state, json!({"type":"unknown"})).is_err());
        assert!(apply_command(&mut state, json!({"type":"recognition", "data":{"machine_hash":"bad"}})).is_err());
        assert!(state.recognition.is_empty());
    }
}
