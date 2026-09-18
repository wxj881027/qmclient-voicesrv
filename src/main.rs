use clap::Parser;
use flate2::{write::GzEncoder, Compression};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use warp::http::{
    header::{CONNECTION, CONTENT_ENCODING, CONTENT_TYPE, VARY},
    HeaderValue, StatusCode,
};
use warp::Filter;
use warp::Reply;

mod realtime;

const VOICE_MAGIC: [u8; 4] = *b"RV01";
const VOICE_TYPE_AUDIO: u8 = 1;
const VOICE_TYPE_PING: u8 = 2;
const VOICE_TYPE_PONG: u8 = 3;
const VOICE_FLAG_LOOPBACK: u8 = 1 << 1;
const TYPE_OFFSET: usize = 5;
const HEADER_SIZE: usize = 4 + 1 + 1 + 2 + 4 + 4 + 1 + 2 + 2 + 4 + 4;
const MAX_PACKET_SIZE: usize = 1200;
const SOCKET_POLL_INTERVAL: Duration = Duration::from_millis(250);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const MAX_CLIENTS: i32 = 64;
const MAX_PLAYER_NAME_LEN: usize = 32;

#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version,
    about = "QmClient integrated voice and identity service"
)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:9987")]
    bind: SocketAddr,

    #[arg(long, default_value_t = 180)]
    idle_timeout_secs: u64,

    #[arg(long, default_value_t = 75)]
    recognition_timeout_secs: u64,

    #[arg(long, default_value_t = 4096)]
    max_clients: usize,

    #[arg(long, default_value_t = 30)]
    stats_interval_secs: u64,

    #[arg(long, default_value = "change-me-in-production")]
    identity_secret: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RoomKey {
    version: u8,
    context_hash: u32,
    token_hash: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PacketType {
    Audio,
    Ping,
    Pong,
}

#[derive(Debug, Clone, Copy)]
struct PacketHeader {
    room: RoomKey,
    packet_type: PacketType,
    flags: u8,
    sender_id: u16,
    sequence: u16,
    payload_size: u16,
}

impl PacketHeader {
    fn parse(packet: &[u8]) -> Result<Self, PacketError> {
        if packet.len() < HEADER_SIZE {
            return Err(PacketError::TooShort);
        }
        if packet.len() > MAX_PACKET_SIZE {
            return Err(PacketError::TooLarge);
        }
        if packet[0..4] != VOICE_MAGIC {
            return Err(PacketError::BadMagic);
        }

        let packet_type = match packet[TYPE_OFFSET] {
            VOICE_TYPE_AUDIO => PacketType::Audio,
            VOICE_TYPE_PING => PacketType::Ping,
            VOICE_TYPE_PONG => PacketType::Pong,
            other => return Err(PacketError::UnknownType(other)),
        };

        let payload_size = read_u16(packet, 6);
        let expected_len = HEADER_SIZE
            .checked_add(usize::from(payload_size))
            .ok_or(PacketError::LengthOverflow)?;
        if expected_len != packet.len() {
            return Err(PacketError::PayloadSizeMismatch {
                declared: payload_size,
                actual: packet.len().saturating_sub(HEADER_SIZE),
            });
        }

        let context_hash = read_u32(packet, 8);
        if context_hash == 0 {
            return Err(PacketError::EmptyContext);
        }

        Ok(Self {
            room: RoomKey {
                version: packet[4],
                context_hash,
                token_hash: read_u32(packet, 12),
            },
            packet_type,
            flags: packet[16],
            sender_id: read_u16(packet, 17),
            sequence: read_u16(packet, 19),
            payload_size,
        })
    }

    fn allows_loopback(self) -> bool {
        (self.flags & VOICE_FLAG_LOOPBACK) != 0
    }
}

#[derive(Debug)]
enum PacketError {
    TooShort,
    TooLarge,
    BadMagic,
    UnknownType(u8),
    LengthOverflow,
    PayloadSizeMismatch { declared: u16, actual: usize },
    EmptyContext,
}

impl fmt::Display for PacketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PacketError::TooShort => write!(f, "packet too short"),
            PacketError::TooLarge => write!(f, "packet too large"),
            PacketError::BadMagic => write!(f, "invalid packet magic"),
            PacketError::UnknownType(value) => write!(f, "unknown packet type {value}"),
            PacketError::LengthOverflow => write!(f, "packet length overflow"),
            PacketError::PayloadSizeMismatch { declared, actual } => {
                write!(
                    f,
                    "payload size mismatch: declared {declared} bytes, actual {actual} bytes"
                )
            }
            PacketError::EmptyContext => write!(f, "empty context hash"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PeerSession {
    room: RoomKey,
    last_seen: Instant,
}

#[derive(Debug, Default)]
struct Stats {
    packets_rx: u64,
    packets_tx: u64,
    audio_rx: u64,
    audio_tx: u64,
    ping_rx: u64,
    pong_tx: u64,
    dropped_packets: u64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct RecognitionKey {
    server_address: String,
    identity: RecognitionIdentity,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum RecognitionIdentity {
    PlayerName(String),
    PlayerId(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientType {
    Qm,
    Arg,
}

impl ClientType {
    fn as_str(self) -> &'static str {
        match self {
            ClientType::Qm => "qm",
            ClientType::Arg => "arg",
        }
    }
}

#[derive(Debug, Clone)]
struct RecognitionRecord {
    qid: String,
    server_address: String,
    player_name: Option<String>,
    player_id: Option<i32>,
    client_type: ClientType,
    dummy: bool,
    foot_particles_enabled: bool,
    remote_particles_enabled: bool,
    voice_supported: bool,
    last_ip: Option<String>,
    last_seen: Instant,
    last_seen_unix: i64,
}

#[derive(Debug, Clone, Serialize)]
struct RecognitionUser {
    server_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    player_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    player_id: Option<i32>,
    dummy: bool,
    client_type: &'static str,
    r#type: &'static str,
    qid: String,
    client_id: String,
    foot_particles_enabled: bool,
    remote_particles_enabled: bool,
    voice_supported: bool,
    last_seen: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_ip: Option<String>,
}

#[derive(Debug)]
struct SharedState {
    identity_secret: String,
    recognition_timeout: Duration,
    recognition: HashMap<RecognitionKey, RecognitionRecord>,
    realtime_changed: tokio::sync::watch::Sender<u64>,
}

impl SharedState {
    fn new(identity_secret: String, recognition_timeout: Duration) -> Self {
        Self {
            identity_secret,
            recognition_timeout,
            recognition: HashMap::new(),
            realtime_changed: tokio::sync::watch::channel(0).0,
        }
    }

    fn cleanup(&mut self, now: Instant) {
        let previous_count = self.recognition.len();
        self.recognition
            .retain(|_, record| record.last_seen + self.recognition_timeout > now);
        if self.recognition.len() != previous_count {
            self.realtime_changed.send_modify(|revision| *revision += 1);
        }
    }

    fn issue_token(&self, machine_hash: &str) -> Result<String, String> {
        let normalized = normalize_machine_hash(machine_hash)?;
        Ok(derive_qid(&self.identity_secret, &normalized))
    }

    fn apply_report(
        &mut self,
        now: Instant,
        request: RecognitionReportRequest,
        remote_ip: Option<IpAddr>,
    ) -> Result<String, String> {
        self.cleanup(now);

        let server_address = request.server_address.trim();
        if server_address.is_empty() {
            return Err("server_address is required".to_string());
        }

        let machine_hash = normalize_machine_hash(&request.machine_hash)?;
        let expected_auth = derive_qid(&self.identity_secret, &machine_hash);
        if request.auth_token.trim() != expected_auth {
            return Err("auth_token does not match machine_hash".to_string());
        }

        let report_client_type =
            normalize_client_type(request.client_type.as_deref()).unwrap_or(ClientType::Qm);
        let valid_players: Vec<_> = request
            .players
            .into_iter()
            .filter_map(|player| {
                let player_name =
                    normalize_player_name(player.player_name.as_deref().or(player.name.as_deref()));
                let player_id = normalize_player_id(player.player_id);
                let identity = if let Some(player_name) = &player_name {
                    RecognitionIdentity::PlayerName(player_name.clone())
                } else if let Some(player_id) = player_id {
                    RecognitionIdentity::PlayerId(player_id)
                } else {
                    return None;
                };
                let client_type = normalize_client_type(
                    player.client_type.as_deref().or(player.r#type.as_deref()),
                )
                .unwrap_or(report_client_type);
                Some((identity, player_name, player_id, client_type, player))
            })
            .collect();
        if valid_players.is_empty() {
            return Err(
                "players must contain at least one valid player_name or legacy player_id"
                    .to_string(),
            );
        }

        let active_identities: HashSet<RecognitionIdentity> = valid_players
            .iter()
            .map(|(identity, _, _, _, _)| identity.clone())
            .collect();
        let qid = expected_auth.clone();
        let previous_count = self.recognition.len();
        self.recognition.retain(|key, record| {
            if record.last_seen + self.recognition_timeout <= now {
                return false;
            }
            if key.server_address == server_address
                && record.qid == qid
                && !active_identities.contains(&key.identity)
            {
                return false;
            }
            true
        });

        let last_ip = remote_ip.map(|ip| ip.to_string());
        let now_unix = unix_now();
        let mut changed = self.recognition.len() != previous_count;
        for (identity, player_name, player_id, client_type, player) in valid_players {
            let key = RecognitionKey {
                server_address: server_address.to_string(),
                identity,
            };
            // 续租本身不触发全量广播；公开状态变化立即推送，租约快照每十秒推送。
            changed |= self.recognition.get(&key).is_none_or(|record| {
                record.qid != qid || record.player_name != player_name || record.player_id != player_id ||
                    record.client_type != client_type || record.dummy != player.dummy.unwrap_or(false) ||
                    record.foot_particles_enabled != player.foot_particles_enabled.unwrap_or(false) ||
                    record.remote_particles_enabled != player.remote_particles_enabled.unwrap_or(false) ||
                    record.voice_supported != player.voice_supported.unwrap_or(true)
            });
            self.recognition.insert(
                key,
                RecognitionRecord {
                    qid: qid.clone(),
                    server_address: server_address.to_string(),
                    player_name,
                    player_id,
                    client_type,
                    dummy: player.dummy.unwrap_or(false),
                    foot_particles_enabled: player.foot_particles_enabled.unwrap_or(false),
                    remote_particles_enabled: player.remote_particles_enabled.unwrap_or(false),
                    voice_supported: player.voice_supported.unwrap_or(true),
                    last_ip: last_ip.clone(),
                    last_seen: now,
                    last_seen_unix: now_unix,
                },
            );
        }

        if changed {
            self.realtime_changed.send_modify(|revision| *revision += 1);
        }
        Ok(qid)
    }

    fn users(&mut self, now: Instant, server_filter: Option<&str>) -> Vec<RecognitionUser> {
        self.cleanup(now);

        let mut users: Vec<_> = self
            .recognition
            .values()
            .filter(|record| {
                server_filter
                    .map(|server| record.server_address == server)
                    .unwrap_or(true)
            })
            .map(|record| RecognitionUser {
                server_address: record.server_address.clone(),
                player_name: record.player_name.clone(),
                player_id: record.player_id,
                dummy: record.dummy,
                client_type: record.client_type.as_str(),
                r#type: record.client_type.as_str(),
                qid: record.qid.clone(),
                client_id: record.qid.clone(),
                foot_particles_enabled: record.foot_particles_enabled,
                remote_particles_enabled: record.remote_particles_enabled,
                voice_supported: record.voice_supported,
                last_seen: record.last_seen_unix,
                last_ip: record.last_ip.clone(),
            })
            .collect();

        users.sort_by(|left, right| {
            left.server_address
                .cmp(&right.server_address)
                .then(left.player_name.cmp(&right.player_name))
                .then(left.player_id.cmp(&right.player_id))
                .then(left.qid.cmp(&right.qid))
        });
        users
    }

    fn active_user_count(&mut self, now: Instant) -> usize {
        self.cleanup(now);
        self.recognition.len()
    }
}

#[derive(Debug, Deserialize)]
struct TokenQuery {
    machine_hash: String,
}

#[derive(Debug, Deserialize)]
struct UsersQuery {
    server_address: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RecognitionReportRequest {
    server_address: String,
    auth_token: String,
    machine_hash: String,
    client_type: Option<String>,
    timestamp: Option<i64>,
    players: Vec<RecognitionReportPlayer>,
}

#[derive(Debug, Deserialize)]
struct RecognitionReportPlayer {
    player_name: Option<String>,
    name: Option<String>,
    player_id: Option<i32>,
    client_type: Option<String>,
    #[serde(rename = "type")]
    r#type: Option<String>,
    dummy: Option<bool>,
    foot_particles_enabled: Option<bool>,
    remote_particles_enabled: Option<bool>,
    voice_supported: Option<bool>,
}

#[derive(Debug, Serialize)]
struct TokenResponse {
    auth_token: String,
    qid: String,
}

#[derive(Debug, Serialize)]
struct UsersResponse {
    users: Vec<RecognitionUser>,
}

#[derive(Debug, Serialize)]
struct ReportResponse {
    status: &'static str,
    qid: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    recognition_users: usize,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

struct VoiceServer {
    socket: UdpSocket,
    peers: HashMap<SocketAddr, PeerSession>,
    idle_timeout: Duration,
    max_clients: usize,
    stats: Stats,
    next_cleanup_at: Instant,
    stats_interval: Duration,
    next_stats_at: Instant,
    shared_state: Arc<Mutex<SharedState>>,
}

impl VoiceServer {
    fn bind(args: &Args, shared_state: Arc<Mutex<SharedState>>) -> io::Result<Self> {
        let socket = UdpSocket::bind(args.bind)?;
        socket.set_read_timeout(Some(SOCKET_POLL_INTERVAL))?;

        let now = Instant::now();
        Ok(Self {
            socket,
            peers: HashMap::new(),
            idle_timeout: Duration::from_secs(args.idle_timeout_secs.max(1)),
            max_clients: args.max_clients.max(1),
            stats: Stats::default(),
            next_cleanup_at: now + CLEANUP_INTERVAL,
            stats_interval: Duration::from_secs(args.stats_interval_secs.max(1)),
            next_stats_at: now + Duration::from_secs(args.stats_interval_secs.max(1)),
            shared_state,
        })
    }

    fn run(&mut self) -> io::Result<()> {
        let mut buffer = [0_u8; MAX_PACKET_SIZE];
        loop {
            match self.socket.recv_from(&mut buffer) {
                Ok((len, addr)) => {
                    self.handle_packet(addr, &buffer[..len]);
                }
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::WouldBlock
                            | io::ErrorKind::TimedOut
                            | io::ErrorKind::Interrupted
                    ) => {}
                Err(err) => {
                    error!("voice recv failed: {err}");
                    thread::sleep(Duration::from_millis(100));
                }
            }

            let now = Instant::now();
            if now >= self.next_cleanup_at {
                self.cleanup(now);
                self.next_cleanup_at = now + CLEANUP_INTERVAL;
            }
            if now >= self.next_stats_at {
                self.log_stats(now);
                self.next_stats_at = now + self.stats_interval;
            }
        }
    }

    fn handle_packet(&mut self, addr: SocketAddr, packet: &[u8]) {
        self.stats.packets_rx = self.stats.packets_rx.saturating_add(1);
        let header = match PacketHeader::parse(packet) {
            Ok(header) => header,
            Err(err) => {
                self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(1);
                debug!("drop packet from {addr}: {err}");
                return;
            }
        };

        let now = Instant::now();
        if !self.upsert_peer(addr, header.room, now) {
            self.stats.dropped_packets = self.stats.dropped_packets.saturating_add(1);
            warn!("voice peer table is full, dropping packet from {addr}");
            return;
        }

        match header.packet_type {
            PacketType::Ping => {
                self.stats.ping_rx = self.stats.ping_rx.saturating_add(1);
                self.reply_pong(addr, packet, header);
            }
            PacketType::Audio => {
                self.stats.audio_rx = self.stats.audio_rx.saturating_add(1);
                self.relay_audio(addr, packet, header, now);
            }
            PacketType::Pong => {
                debug!(
                    "ignoring client-originated pong from {addr} sender_id={} seq={}",
                    header.sender_id, header.sequence
                );
            }
        }
    }

    fn upsert_peer(&mut self, addr: SocketAddr, room: RoomKey, now: Instant) -> bool {
        if let Some(peer) = self.peers.get_mut(&addr) {
            peer.room = room;
            peer.last_seen = now;
            return true;
        }
        if self.peers.len() >= self.max_clients {
            return false;
        }
        self.peers.insert(
            addr,
            PeerSession {
                room,
                last_seen: now,
            },
        );
        true
    }

    fn reply_pong(&mut self, addr: SocketAddr, packet: &[u8], header: PacketHeader) {
        let mut response = [0_u8; MAX_PACKET_SIZE];
        response[..packet.len()].copy_from_slice(packet);
        response[TYPE_OFFSET] = VOICE_TYPE_PONG;

        if let Err(err) = self.socket.send_to(&response[..packet.len()], addr) {
            warn!(
                "voice pong send failed to {addr}: {err} (sender_id={}, seq={})",
                header.sender_id, header.sequence
            );
            return;
        }

        self.stats.packets_tx = self.stats.packets_tx.saturating_add(1);
        self.stats.pong_tx = self.stats.pong_tx.saturating_add(1);
    }

    fn relay_audio(
        &mut self,
        sender: SocketAddr,
        packet: &[u8],
        header: PacketHeader,
        now: Instant,
    ) {
        let include_self = header.allows_loopback();
        let mut targets = Vec::new();
        for (&addr, peer) in &self.peers {
            if peer.last_seen + self.idle_timeout <= now {
                continue;
            }
            if peer.room != header.room {
                continue;
            }
            if !include_self && addr == sender {
                continue;
            }
            targets.push(addr);
        }

        if targets.is_empty() {
            return;
        }

        for target in targets {
            if let Err(err) = self.socket.send_to(packet, target) {
                warn!(
                    "voice relay failed {} -> {}: {} (sender_id={}, seq={}, payload={})",
                    sender, target, err, header.sender_id, header.sequence, header.payload_size
                );
                continue;
            }
            self.stats.packets_tx = self.stats.packets_tx.saturating_add(1);
            self.stats.audio_tx = self.stats.audio_tx.saturating_add(1);
        }
    }

    fn cleanup(&mut self, now: Instant) {
        let before = self.peers.len();
        self.peers
            .retain(|_, peer| peer.last_seen + self.idle_timeout > now);
        let removed = before.saturating_sub(self.peers.len());
        if removed > 0 {
            info!(
                "cleaned up {} idle voice peer(s), {} remaining",
                removed,
                self.peers.len()
            );
        }

        if let Ok(mut shared_state) = self.shared_state.lock() {
            shared_state.cleanup(now);
        }
    }

    fn log_stats(&self, now: Instant) {
        let rooms = self
            .peers
            .values()
            .map(|peer| peer.room)
            .collect::<HashSet<_>>()
            .len();
        let recognition_users = self
            .shared_state
            .lock()
            .map(|mut state| state.active_user_count(now))
            .unwrap_or(0);

        info!(
            "voice stats peers={} rooms={} recognized={} rx={} tx={} audio_rx={} audio_tx={} ping_rx={} pong_tx={} dropped={}",
            self.peers.len(),
            rooms,
            recognition_users,
            self.stats.packets_rx,
            self.stats.packets_tx,
            self.stats.audio_rx,
            self.stats.audio_tx,
            self.stats.ping_rx,
            self.stats.pong_tx,
            self.stats.dropped_packets
        );
    }
}

fn with_shared_state(
    shared_state: Arc<Mutex<SharedState>>,
) -> impl Filter<Extract = (Arc<Mutex<SharedState>>,), Error = Infallible> + Clone {
    warp::any().map(move || shared_state.clone())
}

async fn handle_token(
    query: TokenQuery,
    _remote_addr: Option<SocketAddr>,
    shared_state: Arc<Mutex<SharedState>>,
) -> Result<impl warp::Reply, Infallible> {
    let result = shared_state
        .lock()
        .map_err(|_| "shared state lock poisoned".to_string())
        .and_then(|state| state.issue_token(&query.machine_hash));

    Ok(match result {
        Ok(qid) => json_reply(
            StatusCode::OK,
            &TokenResponse {
                auth_token: qid.clone(),
                qid,
            },
        ),
        Err(error) => json_reply(StatusCode::BAD_REQUEST, &ErrorResponse { error }),
    })
}

async fn handle_report(
    request: RecognitionReportRequest,
    remote_addr: Option<SocketAddr>,
    shared_state: Arc<Mutex<SharedState>>,
) -> Result<impl warp::Reply, Infallible> {
    debug!(
        "recognition report server={} players={} timestamp={:?}",
        request.server_address,
        request.players.len(),
        request.timestamp
    );

    let result = shared_state
        .lock()
        .map_err(|_| "shared state lock poisoned".to_string())
        .and_then(|mut state| {
            state.apply_report(Instant::now(), request, remote_addr.map(|addr| addr.ip()))
        });

    Ok(match result {
        Ok(qid) => json_reply(StatusCode::OK, &ReportResponse { status: "ok", qid }),
        Err(error) => json_reply(StatusCode::BAD_REQUEST, &ErrorResponse { error }),
    })
}

async fn handle_users(
    query: UsersQuery,
    accept_encoding: Option<String>,
    shared_state: Arc<Mutex<SharedState>>,
) -> Result<impl warp::Reply, Infallible> {
    let result = shared_state
        .lock()
        .map_err(|_| "shared state lock poisoned".to_string())
        .map(|mut state| {
            let users = state.users(Instant::now(), query.server_address.as_deref());
            UsersResponse { users }
        });

    Ok(match result {
        Ok(response) => {
            json_reply_with_encoding(StatusCode::OK, &response, accept_encoding.as_deref())
        }
        Err(error) => json_reply_with_encoding(
            StatusCode::INTERNAL_SERVER_ERROR,
            &ErrorResponse { error },
            accept_encoding.as_deref(),
        ),
    })
}

async fn handle_health(
    shared_state: Arc<Mutex<SharedState>>,
) -> Result<impl warp::Reply, Infallible> {
    let recognition_users = shared_state
        .lock()
        .map(|mut state| state.active_user_count(Instant::now()))
        .unwrap_or(0);

    Ok(json_reply(
        StatusCode::OK,
        &HealthResponse {
            status: "ok",
            recognition_users,
        },
    ))
}

fn http_routes(
    shared_state: Arc<Mutex<SharedState>>,
) -> impl Filter<Extract = (impl Reply,), Error = warp::Rejection> + Clone {
    let token_route = warp::path("qm")
        .and(warp::path("token"))
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::query::<TokenQuery>())
        .and(warp::addr::remote())
        .and(with_shared_state(shared_state.clone()))
        .and_then(handle_token);

    let report_route = warp::path("qm")
        .and(warp::path("report"))
        .and(warp::path::end())
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::addr::remote())
        .and(with_shared_state(shared_state.clone()))
        .and_then(handle_report);

    let users_route = warp::path("qm")
        .and(warp::path("users.json"))
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::query::<UsersQuery>())
        .and(warp::header::optional::<String>("accept-encoding"))
        .and(with_shared_state(shared_state.clone()))
        .and_then(handle_users);

    let health_route = warp::path("healthz")
        .and(warp::path::end())
        .and(warp::get())
        .and(with_shared_state(shared_state))
        .and_then(handle_health);

    token_route
        .or(report_route)
        .or(users_route)
        .or(health_route)
}

fn start_http_server(bind: SocketAddr, shared_state: Arc<Mutex<SharedState>>) {
    let routes = realtime::routes(shared_state.clone()).or(http_routes(shared_state));
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("failed to build tokio runtime");
        runtime.block_on(async move {
            info!("http recognition service listening on http://{}", bind);
            warp::serve(routes).run(bind).await;
        });
    });
}

fn json_reply<T: Serialize>(status: StatusCode, value: &T) -> warp::reply::Response {
    let mut response = warp::reply::with_status(warp::reply::json(value), status).into_response();
    response
        .headers_mut()
        .insert(CONNECTION, HeaderValue::from_static("close"));
    response
}

fn json_reply_with_encoding<T: Serialize>(
    status: StatusCode,
    value: &T,
    accept_encoding: Option<&str>,
) -> warp::reply::Response {
    if !accepts_gzip(accept_encoding) {
        let mut response = json_reply(status, value);
        response
            .headers_mut()
            .insert(VARY, HeaderValue::from_static("accept-encoding"));
        return response;
    }

    let json = serde_json::to_vec(value).expect("JSON response serialization failed");
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder
        .write_all(&json)
        .expect("gzip response compression failed");
    let compressed = encoder.finish().expect("gzip response finalization failed");
    let mut response = warp::reply::Response::new(compressed.into());
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
        .headers_mut()
        .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("accept-encoding"));
    response
        .headers_mut()
        .insert(CONNECTION, HeaderValue::from_static("close"));
    response
}

fn accepts_gzip(accept_encoding: Option<&str>) -> bool {
    let Some(accept_encoding) = accept_encoding else {
        return false;
    };

    accept_encoding.split(',').any(|value| {
        let mut parts = value.trim().split(';');
        if !parts
            .next()
            .is_some_and(|encoding| encoding.trim().eq_ignore_ascii_case("gzip"))
        {
            return false;
        }
        !parts.any(|parameter| {
            parameter
                .trim()
                .strip_prefix("q=")
                .and_then(|quality| quality.parse::<f32>().ok())
                .is_some_and(|quality| quality == 0.0)
        })
    })
}

fn normalize_machine_hash(machine_hash: &str) -> Result<String, String> {
    let normalized = machine_hash.trim().to_ascii_lowercase();
    if normalized.len() != 64 || !normalized.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("machine_hash must be a 64-character lowercase hex SHA256 string".to_string());
    }
    Ok(normalized)
}

fn normalize_player_name(player_name: Option<&str>) -> Option<String> {
    let normalized: String = player_name?
        .trim()
        .chars()
        .take(MAX_PLAYER_NAME_LEN)
        .collect();
    if normalized.is_empty() {
        return None;
    }
    Some(normalized)
}

fn normalize_player_id(player_id: Option<i32>) -> Option<i32> {
    match player_id {
        Some(player_id) if (0..MAX_CLIENTS).contains(&player_id) => Some(player_id),
        _ => None,
    }
}

fn normalize_client_type(client_type: Option<&str>) -> Option<ClientType> {
    match client_type?.trim().to_ascii_lowercase().as_str() {
        "arg" | "arghena" => Some(ClientType::Arg),
        "qm" | "qmclient" | "q1meng" => Some(ClientType::Qm),
        _ => None,
    }
}

fn derive_qid(secret: &str, machine_hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hasher.update(b"|");
    hasher.update(machine_hash.as_bytes());
    lower_hex(&hasher.finalize()[..16])
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn read_u16(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}

fn read_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

fn main() -> io::Result<()> {
    env_logger::init();
    let args = Args::parse();

    let shared_state = Arc::new(Mutex::new(SharedState::new(
        args.identity_secret.clone(),
        Duration::from_secs(args.recognition_timeout_secs.max(1)),
    )));

    let mut server = VoiceServer::bind(&args, shared_state.clone())?;
    start_http_server(args.bind, shared_state);

    info!("voice relay listening on udp://{}", args.bind);
    info!(
        "voice relay config idle_timeout={}s recognition_timeout={}s max_clients={} stats_interval={}s",
        args.idle_timeout_secs.max(1),
        args.recognition_timeout_secs.max(1),
        args.max_clients.max(1),
        args.stats_interval_secs.max(1)
    );

    server.run()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_packet(
        version: u8,
        packet_type: u8,
        context_hash: u32,
        token_hash: u32,
        flags: u8,
        sender_id: u16,
        sequence: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut packet = Vec::with_capacity(HEADER_SIZE + payload.len());
        packet.extend_from_slice(&VOICE_MAGIC);
        packet.push(version);
        packet.push(packet_type);
        packet.extend_from_slice(&(payload.len() as u16).to_le_bytes());
        packet.extend_from_slice(&context_hash.to_le_bytes());
        packet.extend_from_slice(&token_hash.to_le_bytes());
        packet.push(flags);
        packet.extend_from_slice(&sender_id.to_le_bytes());
        packet.extend_from_slice(&sequence.to_le_bytes());
        packet.extend_from_slice(&1.0_f32.to_bits().to_le_bytes());
        packet.extend_from_slice(&2.0_f32.to_bits().to_le_bytes());
        packet.extend_from_slice(payload);
        packet
    }

    #[test]
    fn parses_ping_packet() {
        let packet = build_packet(3, VOICE_TYPE_PING, 0x11223344, 0x55667788, 0, 0, 42, &[]);
        let header = PacketHeader::parse(&packet).expect("packet should parse");
        assert_eq!(header.packet_type, PacketType::Ping);
        assert_eq!(header.room.version, 3);
        assert_eq!(header.room.context_hash, 0x11223344);
        assert_eq!(header.room.token_hash, 0x55667788);
        assert_eq!(header.sequence, 42);
        assert_eq!(header.payload_size, 0);
    }

    #[test]
    fn rejects_payload_size_mismatch() {
        let mut packet = build_packet(3, VOICE_TYPE_AUDIO, 1, 2, 0, 7, 9, &[1, 2, 3]);
        packet[6] = 10;
        packet[7] = 0;
        let error = PacketHeader::parse(&packet).expect_err("packet should fail");
        assert!(matches!(error, PacketError::PayloadSizeMismatch { .. }));
    }

    #[test]
    fn rejects_empty_context() {
        let packet = build_packet(3, VOICE_TYPE_PING, 0, 0, 0, 0, 0, &[]);
        let error = PacketHeader::parse(&packet).expect_err("packet should fail");
        assert!(matches!(error, PacketError::EmptyContext));
    }

    #[test]
    fn loopback_flag_is_detected() {
        let packet = build_packet(3, VOICE_TYPE_AUDIO, 1, 2, VOICE_FLAG_LOOPBACK, 7, 9, &[1]);
        let header = PacketHeader::parse(&packet).expect("packet should parse");
        assert!(header.allows_loopback());
    }

    #[test]
    fn qid_derivation_is_stable() {
        let qid1 = derive_qid(
            "secret",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        let qid2 = derive_qid(
            "secret",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        assert_eq!(qid1, qid2);
    }

    #[test]
    fn recognition_report_is_stored_and_filtered() {
        let mut state = SharedState::new("secret".to_string(), Duration::from_secs(60));
        let machine_hash =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string();
        let qid = state.issue_token(&machine_hash).expect("token");
        let request = RecognitionReportRequest {
            server_address: "127.0.0.1:8303".to_string(),
            auth_token: qid.clone(),
            machine_hash,
            client_type: Some("arg".to_string()),
            timestamp: Some(123),
            players: vec![RecognitionReportPlayer {
                player_name: Some("ArgSeven".to_string()),
                name: None,
                player_id: None,
                client_type: None,
                r#type: None,
                dummy: Some(false),
                foot_particles_enabled: Some(true),
                remote_particles_enabled: Some(true),
                voice_supported: Some(true),
            }],
        };

        let stored_qid = state
            .apply_report(Instant::now(), request, None)
            .expect("report");
        assert_eq!(stored_qid, qid);

        let users = state.users(Instant::now(), Some("127.0.0.1:8303"));
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].player_name.as_deref(), Some("ArgSeven"));
        assert_eq!(users[0].player_id, None);
        assert_eq!(users[0].client_type, "arg");
        assert_eq!(users[0].r#type, "arg");
        assert_eq!(users[0].client_id, qid);
        assert!(users[0].voice_supported);
    }

    #[test]
    fn legacy_recognition_report_with_player_id_is_stored() {
        let mut state = SharedState::new("secret".to_string(), Duration::from_secs(60));
        let machine_hash =
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789".to_string();
        let qid = state.issue_token(&machine_hash).expect("token");
        let request = RecognitionReportRequest {
            server_address: "127.0.0.1:8303".to_string(),
            auth_token: qid.clone(),
            machine_hash,
            client_type: Some("qm".to_string()),
            timestamp: Some(123),
            players: vec![RecognitionReportPlayer {
                player_name: None,
                name: None,
                player_id: Some(7),
                client_type: None,
                r#type: None,
                dummy: Some(false),
                foot_particles_enabled: Some(true),
                remote_particles_enabled: Some(true),
                voice_supported: Some(true),
            }],
        };

        let stored_qid = state
            .apply_report(Instant::now(), request, None)
            .expect("report");
        assert_eq!(stored_qid, qid);

        let users = state.users(Instant::now(), Some("127.0.0.1:8303"));
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].player_name, None);
        assert_eq!(users[0].player_id, Some(7));
        assert_eq!(users[0].client_type, "qm");
    }

    #[test]
    fn users_response_is_compressed_and_closes_the_connection() {
        let state = Arc::new(Mutex::new(SharedState::new(
            "secret".to_string(),
            Duration::from_secs(60),
        )));
        let routes = http_routes(state);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .expect("test runtime");

        let response = runtime.block_on(
            warp::test::request()
                .method("GET")
                .path("/qm/users.json")
                .header("accept-encoding", "deflate, gzip")
                .reply(&routes),
        );

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-encoding"], "gzip");
        assert_eq!(response.headers()["connection"], "close");

        let plain_response = runtime.block_on(
            warp::test::request()
                .method("GET")
                .path("/qm/users.json")
                .reply(&routes),
        );

        assert_eq!(plain_response.status(), StatusCode::OK);
        assert!(plain_response.headers().get("content-encoding").is_none());
        assert_eq!(plain_response.headers()["connection"], "close");
        assert_eq!(plain_response.body(), r#"{"users":[]}"#);
    }
}
