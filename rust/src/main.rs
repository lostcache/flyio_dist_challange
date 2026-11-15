use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Write;
use std::fs::File;
use std::io::{self, BufRead, Read};

const MAX_NODES: usize = 128;
const JSON_MSG_BUF_LEN: usize = 65536;
const MAX_NODE_NAME_LEN: usize = 16;
const MAX_PENDING_KV_ADD: usize = 256;
const MAX_BROADCAST_MESSAGES: usize = 4096;
const MAX_OUTGOING: usize = MAX_PENDING_KV_ADD + 1;
const PENDING_READ_TIMEOUT_MS: usize = 5000;
const MAX_LOGS: usize = 1024;
const MAX_LOG_KEY_LEN: usize = 16;
const LOG_RING_BUF_SIZE: usize = 128;

struct Rng {
    file: File,
}

impl Rng {
    fn new() -> io::Result<Self> {
        Ok(Self {
            file: File::open("/dev/urandom")?,
        })
    }

    fn fill(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.file.read_exact(buf)
    }
}

#[derive(Deserialize, Serialize)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Body {
    Init {
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    },
    Echo {
        msg_id: u64,
        echo: String,
    },
    Generate {
        msg_id: u64,
    },
    Topology {
        msg_id: u64,
        topology: serde_json::Value,
    },
    Broadcast {
        msg_id: u64,
        message: u64,
    },
    Read(ReadBody),
    Add {
        msg_id: u64,
        delta: u64,
    },
    Write {
        key: String,
        value: u64,
        msg_id: u64,
    },
    InitOk {
        in_reply_to: u64,
    },
    EchoOk {
        in_reply_to: u64,
        echo: String,
    },
    GenerateOk {
        in_reply_to: u64,
        id: String,
    },
    TopologyOk {
        in_reply_to: u64,
    },
    BroadcastOk {
        in_reply_to: u64,
    },
    ReadOk(ReadOkBody),
    WriteOk {
        in_reply_to: u64,
    },
    Error {
        code: u64,
        text: String,
        in_reply_to: u64,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum ReadBody {
    Kv { key: String, msg_id: u64 },
    Broadcast { msg_id: u64 },
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum ReadOkBody {
    Broadcast {
        in_reply_to: u64,
        messages: Vec<u64>,
    },
    Kv {
        in_reply_to: u64,
        value: u64,
    },
}

// struct BroadcastMessage {}
// struct LogBuffers {}
//
// struct KVStore {}
//
// struct PandingKVAdd {}
// struct PendingKVReadAll {}

#[derive(Debug)]
enum Fatal {
    Oom,
    Io,
}

struct LocalKvStore {
    store: HashMap<String, u64>,
}

#[derive(Clone)]
struct PendingKvAdd {
    client_msg_id: u64,
    kv_msg_id: u64,
}

struct PendingKvRead {}

struct Ctx {
    node_id: String,
    node_id_set: bool,
    node_ids: Vec<String>,
    init_msg_id: Option<u64>,
    next_msg_id: u64,
    broadcast_messages: Vec<u64>,
    pending_kv_add: HashMap<u64, HashMap<u64, ()>>,
    clientread_readres_pendingreads: HashMap<u64, (u64, u64)>,
    kvread_clientread: HashMap<u64, u64>,
    local_counter_init_kv_read_msg_id: Option<u64>,
    local_counter: u64,
    // lb: LogBuffers,
    // kvs: KVStore,
    // panding_adds: Vec<PandingKVAdd>,
    // pending_kv_read_alls: Vec<PendingKVReadAll>,
}

impl Ctx {
    fn new() -> Result<Self, Fatal> {
        let mut node_id = String::new();
        node_id
            .try_reserve(MAX_NODE_NAME_LEN)
            .map_err(|_| Fatal::Oom);

        let mut node_ids = Vec::new();
        node_ids
            .try_reserve_exact(MAX_NODES)
            .map_err(|_| Fatal::Oom);

        let mut broadcast_messages = Vec::new();
        broadcast_messages
            .try_reserve_exact(MAX_BROADCAST_MESSAGES)
            .map_err(|_| Fatal::Oom);

        Ok(Ctx {
            node_id,
            node_id_set: false,
            node_ids,
            init_msg_id: None,
            next_msg_id: 0,
            broadcast_messages,
            pending_kv_add: HashMap::new(),
            clientread_readres_pendingreads: HashMap::new(),
            kvread_clientread: HashMap::new(),
            local_counter: 0,
            local_counter_init_kv_read_msg_id: None,
        })
    }
}

fn send_kv_read_to_init_local_counter(ctx: &mut Ctx) {
    let msg_id = ctx.next_msg_id;
    ctx.next_msg_id += 1;
    send(
        ctx.node_id.to_string(),
        "kv_seq".to_string(),
        Body::Read(ReadBody::Kv {
            key: ctx.node_id.clone(),
            msg_id,
        }),
    );
    ctx.local_counter_init_kv_read_msg_id = Some(msg_id);
}

fn handle_init(ctx: &mut Ctx, msg_id: u64, node_id: String, node_ids: Vec<String>) -> Option<Body> {
    if node_ids.len() > MAX_NODES {
        return None;
    }

    ctx.node_id = node_id;
    ctx.node_id_set = true;
    ctx.node_ids.clear();
    ctx.node_ids.extend(node_ids);
    ctx.init_msg_id = Some(msg_id);

    Some(Body::InitOk {
        in_reply_to: msg_id,
    })
}

fn handle_echo(_ctx: &mut Ctx, msg_id: u64, echo: String) -> Option<Body> {
    Some(Body::EchoOk {
        in_reply_to: msg_id,
        echo,
    })
}

fn handle_generate(_ctx: &mut Ctx, msg_id: u64, rng: &mut Rng) -> Option<Body> {
    let mut id_buf: [u8; 64] = [0; 64];
    rng.fill(&mut id_buf).ok()?;
    let mut id = String::with_capacity(2 * 64);
    for byte in id_buf {
        write!(&mut id, "{byte:02x}").ok()?;
    }

    Some(Body::GenerateOk {
        in_reply_to: msg_id,
        id,
    })
}

fn handle_topology(_ctx: &mut Ctx, msg_id: u64) -> Option<Body> {
    Some(Body::TopologyOk {
        in_reply_to: msg_id,
    })
}

fn handle_broadcast(ctx: &mut Ctx, msg_id: u64, message: u64) -> Option<Body> {
    if ctx.broadcast_messages.len() >= MAX_BROADCAST_MESSAGES {
        return None;
    }
    ctx.broadcast_messages.push(message);

    Some(Body::BroadcastOk {
        in_reply_to: msg_id,
    })
}

fn handle_broadcast_read(ctx: &mut Ctx, msg_id: u64) -> Option<Body> {
    Some(Body::ReadOk(ReadOkBody::Broadcast {
        in_reply_to: msg_id,
        messages: ctx.broadcast_messages.clone(),
    }))
}

fn handle_kv_read_reply(ctx: &mut Ctx, value: u64, in_reply_to: u64) -> Option<Body> {
    let Some(mid) = ctx.local_counter_init_kv_read_msg_id else {
        return None;
    };

    let Some(init_idx) = ctx.init_msg_id else {
        return None;
    };

    if mid == in_reply_to {
        return Some(Body::InitOk {
            in_reply_to: init_idx,
        });
    }

    return None;
}

// fn handle_add(ctx: &mut Ctx, src: &String, msg_id: u64, _delta: u64) -> Option<serde_json::Value> {
//     for pe in ctx.pending_kv_add.iter_mut() {
//         if pe.used {
//             continue;
//         }
//
//         let res_msg_id = ctx.next_msg_id;
//         ctx.next_msg_id += 1;
//
//         pe.used = true;
//         pe.client = src.clone();
//         pe.client_msg_id = msg_id;
//         pe.kv_msg_id = res_msg_id;
//
//         // return serde_json::to_value().ok()?;
//     }
//     None
// }

fn send(src: String, dest: String, body: Body) {
    if let Ok(str_res) = serde_json::to_string(&Message { src, dest, body }) {
        println!("{}", str_res);
    };
}

fn main() -> Result<(), Fatal> {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut msg_buf = Vec::new();
    msg_buf
        .try_reserve(JSON_MSG_BUF_LEN)
        .map_err(|_| Fatal::Oom)?;

    let mut ctx = Ctx::new().expect("could not create context");

    let Ok(mut rng) = Rng::new() else {
        return Err(Fatal::Io);
    };

    loop {
        msg_buf.clear();

        match input.read_until(b'\n', &mut msg_buf) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(err) => {
                eprintln!("couldn't read stdin: {err}");
                return Ok(());
            }
        }

        let msg: Message = match serde_json::from_slice(&msg_buf) {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!("couldn't parse json: {err}");
                continue;
            }
        };

        let Message { src, dest, body } = msg;

        let res_body = match body {
            Body::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                let res = handle_init(&mut ctx, msg_id, node_id, node_ids);
                send_kv_read_to_init_local_counter(&mut ctx);
                res
            }
            Body::Echo { msg_id, echo } => handle_echo(&mut ctx, msg_id, echo),
            Body::Generate { msg_id } => handle_generate(&mut ctx, msg_id, &mut rng),
            Body::Topology {
                msg_id,
                topology: _topology,
            } => handle_topology(&mut ctx, msg_id),
            Body::Broadcast { msg_id, message } => handle_broadcast(&mut ctx, msg_id, message),
            Body::Read(read) => match read {
                ReadBody::Broadcast { msg_id } => handle_broadcast_read(&mut ctx, msg_id),
                ReadBody::Kv { .. } => None,
            },
            Body::Add { .. } => None,
            Body::Write { .. } => None,
            Body::ReadOk(read_ok) => match read_ok {
                ReadOkBody::Broadcast { .. } => None,
                ReadOkBody::Kv { value, in_reply_to } => {
                    handle_kv_read_reply(&mut ctx, value, in_reply_to)
                }
            },
            Body::WriteOk { .. } => None,
            Body::Error { .. } => None,
            // Outbound-only responses. This node sends these. It never receives them.
            Body::InitOk { .. }
            | Body::EchoOk { .. }
            | Body::GenerateOk { .. }
            | Body::TopologyOk { .. }
            | Body::BroadcastOk { .. } => None,
        };

        if let Some(res_body) = res_body {
            send(dest, src, res_body);
        };
    }
}
