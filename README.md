# qmclient-voicesrv

QmClient（栖梦，基于 DDNet 的第三方定制客户端）的**语音中继与身份识别服务**。Rust 实现，单进程同时提供 UDP 语音中继与 HTTP/WebSocket 身份接口，已在生产环境运行。

## HTTP 公开访问边界

`/qm/token`、`/qm/report`、`/qm/users.json` 已删除，直接请求返回 404。
`/healthz` 只接受回环来源，公网请求同样返回 404；身份与在线同步继续走仅限回环访问的 `/qm/realtime` WebSocket。

## 简介

QmClient 的语音功能需要两类基础设施：

1. **语音数据面**：玩家说出的 Opus 语音包需要被转发给同一局游戏里的其他玩家。DDNet 本身不提供这样的通道。
2. **身份与控制面**：客户端需要一个跨设备稳定的身份，以及「当前这台游戏服务器上有哪些 QmClient 玩家、谁支持语音、谁开了脚部粒子/远程尾迹」这类在线状态，用于名牌标记、图标与语音可用性提示。

`voicesrv` 用一个进程同时提供这两件事：

- 在 `--bind` 指定的地址上监听 **UDP**，按房间转发语音包，并应答延迟探测。服务端不解码、不重编码 Opus，payload 原样转发。
- 在同一地址的 **TCP** 端口上用 warp 提供 HTTP/WebSocket 接口：`GET /healthz` 只供本机健康检查、`GET /qm/realtime`（WebSocket）向同机中心服推送在线变化。

运行模型：主线程跑阻塞式 UDP 循环（收包、转发、peer 清理、统计日志），另起一个线程跑 tokio multi-thread runtime 承载 HTTP 与 WebSocket；两者通过 `Arc<Mutex<SharedState>>` 共享身份密钥与在线记录。全部状态都在内存里，不依赖数据库或外部缓存。

```
src/main.rs                              UDP 语音中继 + HTTP 路由 + 身份/在线状态（含单元测试）
src/realtime.rs                          中心服专用回环 WebSocket 通道（含单元测试）
deploy/systemd/qmclient-voicesrv.service systemd 单元
Cargo.toml                               crate 定义与依赖
```

## 功能特性

**语音中继（UDP）**

- 解析并校验 `RV01` 报文头：magic、类型、`payload_size` 与实际长度一致性、`context_hash` 非 0、整包不超过 1200 字节。
- 房间隔离：以 `version + context_hash + token_hash` 为房间键，不同协议版本、不同服务器上下文、不同房间口令的玩家互不可见。
- `PING` 探测：服务端把整包回显，仅把 `type` 改成 `PONG`，客户端据此测量往返时延。
- 可选自回环：客户端置位 `LOOPBACK` flag 时，`AUDIO` 也会转发回发送方（用于本地回声测试）。
- 空闲 peer 清理：超过 `--idle-timeout-secs` 未再发包的 UDP 端点被移出转发表。
- 转发表上限：超过 `--max-clients` 后来自新地址的包被丢弃并告警。
- 不解析 Opus payload；`sender_id`、`seq`、`pos_x`、`pos_y` 一律原样转发。
- 周期性输出运行统计（peers / rooms / rx / tx / dropped 等）。

**身份与在线同步（回环 WebSocket）**

- 由客户端 `machine_hash` 与 `--identity-secret` 稳定派生 QID：同一台机器、同一密钥，结果始终一致。
- 按 `server_address` + 玩家身份记录在线状态（玩家名优先，兼容旧的 `player_id` 形式）。
- 记录能力位：`dummy`、`foot_particles_enabled`、`remote_particles_enabled`、`voice_supported`、`client_type`（`qm` 或 `arg`）。
- 在线记录带租约：超过 `--recognition-timeout-secs` 未续租即过期；同一 QID 在同一服务器上「本次上报未再出现」的身份会被立即删除，改名/退出即时生效。
- 中心服回环 WebSocket 推送在线快照：状态变化后合并 100 ms 再推送，另有每 10 秒的兜底全量快照。

## 协议说明（UDP 语音中继）

### 报文头布局

所有字段小端（little-endian），头部固定 29 字节，紧随其后是 `payload_size` 字节的 Opus payload：

```
偏移   长度   类型       字段             说明
------ ------ ---------- ---------------- --------------------------------------------
0      4      [u8; 4]    magic            固定 ASCII "RV01"
4      1      u8         version          协议版本，参与房间键
5      1      u8         type             1 = AUDIO，2 = PING，3 = PONG
6      2      u16        payload_size     payload 字节数，必须等于 整包长度 - 29
8      4      u32        context_hash     游戏服务器上下文哈希，0 视为非法包
12     4      u32        token_hash       房间口令哈希，0 表示无口令房间
16     1      u8         flags            bit1（0x02）= LOOPBACK，其余位保留
17     2      u16        sender_id        发送方 ID，服务端不改写
19     2      u16        seq              序号，服务端不改写
21     4      f32        pos_x            发送方 X 坐标，服务端不改写
25     4      f32        pos_y            发送方 Y 坐标，服务端不改写
------ ------ ---------- ---------------- --------------------------------------------
合计 29 字节头部，整包上限 1200 字节（payload 最大 1171 字节）
```

注意：服务端**不读取** `pos_x` / `pos_y`，也不使用 `sender_id` / `seq` 做任何路由或去重判断——它们只是被原样转发。距离衰减、立体声定位、黑白名单、VAD 等全部由客户端本地决定。

### 报文类型行为

| type | 名称 | 服务端行为 |
| ---- | ---- | ---------- |
| 1 | `AUDIO` | 转发给同一房间内所有活跃 peer；发送方默认被排除，除非该包置了 `LOOPBACK` flag |
| 2 | `PING` | 把收到的整包回显给发送方，仅把 `type` 字节改为 3（`PONG`）；payload 与其它字段保持不变 |
| 3 | `PONG` | 客户端发来的 `PONG` 被忽略（仅记 debug 日志） |

### 房间隔离规则

房间键为三元组 `version + context_hash + token_hash`：

- `version`（协议版本）：不同版本的客户端即使在同一台服务器上也不会互相听到。
- `context_hash`（上下文哈希）：区分不同的游戏服务器/地图上下文；为 0 的包被判为非法并丢弃。
- `token_hash`（房间口令哈希）：客户端可选的房间口令；0 表示「无口令房间」，是合法值。

peer 的房间随其最新一个合法包更新——客户端切换房间只需发出一个带新房间键的包，不需要重连。

### LOOPBACK 语义

`flags` 的 bit1（值 `0x02`）为 `LOOPBACK`：

- 置位：`AUDIO` 也会被转发回发送方自己的 UDP 端点，用于「服务器回环」自测。
- 未置位（默认）：发送方不会收到自己的包。

### 丢包与传输语义

- 服务端**不做**重传、不做乱序重排、不做去重、不缓存历史包，也不统计丢包率；一个 `AUDIO` 包只会被转发一次（每个目标一次 `send_to`）。
- 丢包、抖动、乱序由客户端侧处理（Opus 的 FEC / PLC 等），服务端只做无状态的按房间转发。
- `stats` 里的 `dropped` 统计的是**被丢弃的非法包或超限包**（长度不符、magic 错误、`context_hash` 为 0、类型未知、转发表已满），不是网络丢包。
- peer 需要持续发包才能留在转发表中；静默超过 `--idle-timeout-secs` 即被清理（客户端用 `PING` 即可续租）。
- UDP 无加密也无鉴权：房间键只保证「不串台」，不保证机密性，任何知道房间键的人都能加入该房间。不要用语音房间传递敏感信息。

## 内部 HTTP 接口

旧 `/qm/token`、`/qm/report` 与 `/qm/users.json` 已删除。

### `GET /healthz`

无参数、无鉴权的健康检查，适合 systemd/LB 探活。

```json
{
  "status": "ok",
  "recognition_users": 12
}
```

`recognition_users` 是当前未过期的在线记录条数（调用时会先做过期清理）。

### `GET /qm/realtime`（WebSocket）

> 该通道供**同机部署的中心服**使用，不是给客户端直连的接口。

- 只接受来自 loopback 地址的连接；非回环来源返回 404。
- 单条消息上限 64 KiB；服务端发送有 5 秒超时，持续写不动的慢连接会被断开。
- 中心服连上后无需订阅，服务端会主动推送。

服务端 → 中心服（在线快照，字段与 `/qm/users.json` 一致，但**不含 `last_ip`**）：

```json
{
  "type": "users",
  "data": {
    "users": [ { "server_address": "ddnet.example:8303", "player_name": "QmSeven", "qid": "0123456789abcdef0123456789abcdef", "voice_supported": true } ]
  }
}
```

推送时机：在线状态发生变化后合并 100 ms 再推一次全量快照；另有每 10 秒的定时快照兜底（续租本身不触发额外推送）。

中心服 → 服务端（代客户端上报，`data` 就是 `/qm/report` 的请求体，但**不带 `auth_token`**，服务端会用 `identity_secret` 自行派生）：

```json
{
  "type": "recognition",
  "ip": "198.51.100.7",
  "data": {
    "machine_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "server_address": "ddnet.example:8303",
    "players": [
      { "player_name": "QmSeven", "player_id": 3, "voice_supported": true }
    ]
  }
}
```

该入站指令有额外限制：`server_address` 不超过 128 字节，`players` 不超过 2 个（覆盖主号与 dummy），`machine_hash` 必须是 64 位十六进制。

出错时返回：

```json
{ "type": "error", "data": { "error": "invalid_presence" } }
```

错误码：

| 错误码 | 含义 |
| ------ | ---- |
| `invalid_json` | 消息不是合法 JSON |
| `unknown_command` | `type` 不是 `recognition` |
| `invalid_body` | 缺少 `data`，或 `data` 不是 JSON 对象 |
| `invalid_machine_hash` | 缺少 `machine_hash`，或它不是字符串 |
| `invalid_presence` | 上报体反序列化失败、`server_address` 超过 128 字节，或 `players` 超过 2 个 |
| `state_unavailable` | 内部共享状态不可用 |
| 校验错误原文 | 也可能是上报校验失败时返回的原文，如 `machine_hash must be a 64-character lowercase hex SHA256 string`、`server_address is required` |

## 构建与运行

需要较新的 Rust 工具链（edition 2021）。

```sh
# 在仓库根目录
cargo build --release
# 产物：target/release/voicesrv

cargo run --release -- --bind 127.0.0.1:9987 --identity-secret "<本地调试用随机串>"
```

本地调试示例：

```sh
# 健康检查
curl -s http://127.0.0.1:9987/healthz

```

日志用 `env_logger`，通过 `RUST_LOG` 控制级别；未设置时只输出 `error` 级别。

```sh
RUST_LOG=info  ./target/release/voicesrv --bind 0.0.0.0:9987 --identity-secret "<secret>"   # 生产：启动信息 + 周期统计
RUST_LOG=debug ./target/release/voicesrv --bind 127.0.0.1:9987 --identity-secret "<secret>"  # 排查：丢包原因、上报摘要、被忽略的 PONG
```

`info` 级别会输出启动信息（`voice relay listening on udp://...`、`http recognition service listening on http://...`）与周期统计：

```
voice stats peers=<在线端点> rooms=<活跃房间> recognized=<在线记录> rx=<收到包> tx=<发出包> audio_rx=<收到语音> audio_tx=<转发语音> ping_rx=<收到探测> pong_tx=<回复探测> dropped=<丢弃包>
```

依赖（见 `Cargo.toml`）：`tokio`（net / rt-multi-thread / time / sync / macros）、`warp`（`default-features = false`，仅启用 `websocket`）、`serde` / `serde_json`、`sha2`、`clap`（derive）、`env_logger`、`log`、`futures-util`。

## 命令行参数

| 参数 | 默认值 | 说明 |
| ---- | ------ | ---- |
| `--bind <ADDR:PORT>` | `0.0.0.0:9987` | 监听地址。UDP 语音中继与 TCP HTTP/WS 接口共用该地址与端口 |
| `--idle-timeout-secs <N>` | `180` | UDP peer 空闲多久后被移出转发表；内部按最小 1 秒处理 |
| `--recognition-timeout-secs <N>` | `75` | 在线记录的租约时长，超时未续租即过期；内部按最小 1 秒处理 |
| `--max-clients <N>` | `4096` | 转发表中 UDP 端点的上限，超出后丢弃来自新地址的包；内部按最小 1 处理 |
| `--stats-interval-secs <N>` | `30` | 统计日志间隔；内部按最小 1 秒处理 |
| `--identity-secret <SECRET>` | `change-me-in-production` | 派生 QID 的密钥。**生产环境必须显式设置为足够长的随机值** |

`--help` / `--version` 由 clap 提供。

## 部署

Linux 生产部署使用 `deploy/systemd/qmclient-voicesrv.service`。要点：

1. 用无登录的系统用户 `qmvoice` 运行。
2. 二进制放在 `/opt/qmclient-voicesrv/voicesrv`（与该单元的 `WorkingDirectory` 一致）。
3. 同时在防火墙放行**同一个端口的 UDP 与 TCP**——语音走 UDP，HTTP/WS 走 TCP。

```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin qmvoice
sudo install -d -m 0755 /opt/qmclient-voicesrv
sudo install -m 0755 target/release/voicesrv /opt/qmclient-voicesrv/voicesrv
sudo install -m 0644 deploy/systemd/qmclient-voicesrv.service /etc/systemd/system/qmclient-voicesrv.service
sudo systemctl daemon-reload
sudo systemctl enable --now qmclient-voicesrv
systemctl status qmclient-voicesrv
journalctl -u qmclient-voicesrv -f
```

防火墙（ufw 示例，端口与 `--bind` 保持一致）：

```sh
sudo ufw allow 9987/udp
sudo ufw allow 9987/tcp
```

### EnvironmentFile 写法

仓库中的 `deploy/systemd/qmclient-voicesrv.service` 已经把监听地址与密钥移出 `ExecStart`，改用 `EnvironmentFile` 注入，避免密钥出现在单元文件与 `systemctl cat` 输出里：

```ini
[Service]
Type=simple
User=qmvoice
Group=qmvoice
WorkingDirectory=/opt/qmclient-voicesrv

# 前缀 "-" 表示文件缺失时仍可启动
EnvironmentFile=-/etc/qmclient/voicesrv.env

ExecStart=/opt/qmclient-voicesrv/voicesrv --bind ${BIND_ADDR} --identity-secret ${IDENTITY_SECRET}
Restart=always
RestartSec=2

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
```

配套的环境文件内容见 `deploy/voicesrv.env.example`：

```sh
# /etc/qmclient/voicesrv.env
BIND_ADDR=0.0.0.0:9987
IDENTITY_SECRET=REPLACE_WITH_RANDOM_SECRET
RUST_LOG=info
```

安装：先落盘环境文件并收紧权限，再启动服务。

```sh
sudo install -d -m 0755 /etc/qmclient
sudo install -m 0600 -o root -g root deploy/voicesrv.env.example /etc/qmclient/voicesrv.env
sudo editor /etc/qmclient/voicesrv.env   # 填入真实 IDENTITY_SECRET
sudo systemctl restart qmclient-voicesrv
```

注意 `EnvironmentFile` 缺失时单元仍能启动（因为带了 `-` 前缀），但 `${IDENTITY_SECRET}` 会展开成空值，服务将带着默认或空密钥运行——请在启动前确认环境文件已就位。

密钥生成（256 位随机，十六进制表示）：

```sh
openssl rand -hex 32
```

单元设置 `Restart=always` 与 `RestartSec=2`，进程异常退出后会自动拉起；因为状态全在内存中，重启后在线记录会被清空，客户端下一次上报（或中心服的下一轮 presence）会自动重建。

## 安全提示

- **`--identity-secret` 必须改成足够长的随机值**。默认值 `change-me-in-production` 只允许出现在本地调试里：任何人用它加上一个已知的 `machine_hash`，都能直接算出对应的 QID。
- 建议使用 **至少 32 字节**（128 位以上）的密码学随机值，例如 `openssl rand -hex 32` 的输出；不要使用可读短句、不要复用其它服务的密钥。
- **不要把密钥提交到仓库**，也不要写进单元文件的 `ExecStart`（会出现在 `systemctl cat` 与进程列表里）。用权限 `0600`、属主 `root` 的 `EnvironmentFile`，或交给系统的 secret 管理机制。
- 轮换密钥会让**所有 QID 改变**（`qid` 是密钥与 `machine_hash` 的函数），中心服需通过回环通道重新上报身份；请在可以接受在线状态短暂重建的时间窗内轮换。
- 对外不提供 HTTP 身份签发、上报名单或健康状态查询；身份上报仅经回环 WebSocket 通道。
- `GET /qm/realtime` 只按 TCP 源地址判断回环：**同机反向代理会让来源「看起来像」回环**。不要给这个路径配置公网入口。
- 服务自身不终止 TLS（warp 以 `default-features = false` 构建，未启用 `tls` feature）。需要 HTTPS 时请在 TCP 侧套反向代理；UDP 语音不受影响。
- 语音中继是无加密的 UDP 转发。房间键（`version + context_hash + token_hash`）只做隔离，不做鉴权与加密；服务端信任包里携带的 `sender_id`、`seq`、坐标等元数据，客户端不应把这些字段用于任何安全判断。
- 服务端不校验 `server_address` 的形式，也不会主动向游戏服务器核实；它只是上报内容的一部分，用于分组与过滤。

## 与客户端的对接说明

客户端通过中心服的实时 WebSocket 上报身份与在线状态；中心服再通过回环 `/qm/realtime` 连接本进程。
旧 HTTP 识别协议已删除，不再使用 `/qm/token`、`/qm/report` 与 `/qm/users.json`。
健康检查只能从本机回环地址发起，公网请求返回 404。

## 测试

```sh
cargo test
```

单元测试分布在两个 `#[cfg(test)]` 模块里，共 10 个用例。

`src/main.rs`（8 个）：

| 用例 | 覆盖内容 |
| ---- | -------- |
| `parses_ping_packet` | 正常解析 `PING` 包：类型、`version`、`context_hash`、`token_hash`、`seq`、`payload_size` |
| `rejects_payload_size_mismatch` | 头部声明的 `payload_size` 与实际长度不符时报 `PayloadSizeMismatch` |
| `rejects_empty_context` | `context_hash` 为 0 时报 `EmptyContext` |
| `loopback_flag_is_detected` | `LOOPBACK` flag 被正确识别 |
| `qid_derivation_is_stable` | 相同 secret 与 `machine_hash` 派生出的 QID 稳定不变 |
| `recognition_report_is_stored_and_filtered` | 上报写入后可按 `server_address` 过滤查询；校验 `player_name` 身份、`client_type`（`arg`）、`client_id`/`type` 同值、`voice_supported` |
| `legacy_recognition_report_with_player_id_is_stored` | 旧格式（只有 `player_id`）的上报同样被存储，且 `player_name` 保持为空 |
| `public_http_requests_do_not_return_identity_or_health_data` | 公网请求旧 HTTP 入口与健康检查均返回 404，本机健康检查仍可用 |

`src/realtime.rs`（2 个）：

| 用例 | 覆盖内容 |
| ---- | -------- |
| `internal_report_reuses_identity_and_announces_changes` | 内部 `recognition` 指令复用同一身份（QID 由相同设备哈希稳定派生）、触发变更通知，并在租约过期后清空在线记录 |
| `invalid_internal_command_does_not_change_presence` | 未知 `type`、非法 `machine_hash` 等指令不改变在线状态 |

未被上述用例直接断言的解析分支是长度越界（短于 29 字节 / 超过 1200 字节）、magic 错误与未知 `type` 的拒绝路径，它们由 `PacketHeader::parse` 的实现保证。

## 许可证

本项目以 **Zlib** 许可证发布，`Cargo.toml` 中声明为 `license = "Zlib"`；许可证全文见仓库根目录的 `LICENSE`。

Zlib 许可证允许自由使用、修改与再分发（包括商业用途），要求保留版权与许可声明，并声明本软件按「原样」提供、不附带任何明示或暗示的担保。
