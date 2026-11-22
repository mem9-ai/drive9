// /v1/events — per-tenant SSE change stream per drive9-server-api-spec.md:
// - Structural ops (`rename`, `delete`, `mkdir`, `copy`) emit `event: reset`
//   with `reason: "structural_change"`.
// - Content writes emit `event: file_changed` carrying `seq`, `path`, `op`,
//   `actor` (from X-Dat9-Actor), and `ts`.
// - `since == 0` (or omitted) performs initial sync: sends
//   `event: reset` (`reason: "initial_sync"`, `seq: head`) + `heartbeat`.
// - `since > 0`: if covered by the retained ring (up to LOG_CAP = 8192 events,
//   well above the >1000-event paginated backlog test), replays all frames with
//   `seq > since` followed by `heartbeat`; if `since` predates retention or is
//   ahead of `head`, emits `event: reset` (`reason: "seq_too_old"`) + `heartbeat`.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

const LOG_CAP: usize = 8192;
pub const HEARTBEAT_SECS: u64 = 15;

pub enum InitialPlan {
    InitialSync { head: u64 },
    Replay { head: u64, frames: Vec<String> },
    SeqTooOld { head: u64 },
}

pub struct Hub {
    inner: Mutex<HubInner>,
    tx: broadcast::Sender<String>,
}

struct HubInner {
    seq: u64,
    log: VecDeque<(u64, String)>,
}

fn is_structural_op(op: &str) -> bool {
    matches!(op, "rename" | "delete" | "mkdir" | "copy")
}

impl Hub {
    fn new() -> Hub {
        let (tx, _) = broadcast::channel(1024);
        Hub {
            inner: Mutex::new(HubInner {
                seq: 0,
                log: VecDeque::new(),
            }),
            tx,
        }
    }

    pub fn publish(&self, path: &str, op: &str, actor: &str) {
        let frame = {
            let mut g = self.inner.lock().unwrap();
            g.seq += 1;
            let seq = g.seq;
            let frame = if is_structural_op(op) {
                let mut payload = serde_json::json!({
                    "seq": seq,
                    "reason": "structural_change",
                    "path": path,
                    "op": op,
                });
                if !actor.is_empty() {
                    payload["actor"] = serde_json::json!(actor);
                }
                format!("event: reset\ndata: {payload}\n\n")
            } else {
                let payload = serde_json::json!({
                    "seq": seq,
                    "path": path,
                    "op": op,
                    "actor": actor,
                    "ts": crate::server::store::now_millis(),
                });
                format!("event: file_changed\ndata: {payload}\n\n")
            };
            g.log.push_back((seq, frame.clone()));
            if g.log.len() > LOG_CAP {
                g.log.pop_front();
            }
            frame
        };
        let _ = self.tx.send(frame);
    }

    pub fn head(&self) -> u64 {
        self.inner.lock().unwrap().seq
    }

    pub fn plan_connect(&self, since: Option<u64>) -> (InitialPlan, broadcast::Receiver<String>) {
        let g = self.inner.lock().unwrap();
        let rx = self.tx.subscribe();
        let head = g.seq;
        let Some(s) = since else {
            return (InitialPlan::InitialSync { head }, rx);
        };
        if s == 0 {
            return (InitialPlan::InitialSync { head }, rx);
        }
        if s > head {
            return (InitialPlan::SeqTooOld { head }, rx);
        }
        if s == head {
            return (
                InitialPlan::Replay {
                    head,
                    frames: Vec::new(),
                },
                rx,
            );
        }
        let oldest = g.log.front().map(|(seq, _)| *seq).unwrap_or(head + 1);
        if s + 1 < oldest {
            return (InitialPlan::SeqTooOld { head }, rx);
        }
        let frames = g
            .log
            .iter()
            .filter(|(seq, _)| *seq > s)
            .map(|(_, f)| f.clone())
            .collect();
        (InitialPlan::Replay { head, frames }, rx)
    }
}

#[derive(Default)]
pub struct Hubs {
    map: Mutex<HashMap<String, Arc<Hub>>>,
}

impl Hubs {
    pub fn for_tenant(&self, tenant: &str) -> Arc<Hub> {
        let mut map = self.map.lock().unwrap();
        map.entry(tenant.to_string())
            .or_insert_with(|| Arc::new(Hub::new()))
            .clone()
    }

    pub fn drop_tenant(&self, tenant: &str) {
        self.map.lock().unwrap().remove(tenant);
    }
}
