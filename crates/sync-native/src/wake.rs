// wake channel: a plain WebSocket per client to its namespace host. the
// channel carries no data — a wake means only "pull now". after a push
// commits, the host wakes the namespace's OTHER connected clients as a
// post-commit effect. it is advisory and carries zero correctness weight: a
// lost or duplicated wake can never cause missed or wrong data because
// convergence comes entirely from the pull protocol.
//
// coalescing is per-socket via tokio Notify: notify_one stores at most one
// permit, so a burst of pushes collapses into a single pending wake per
// socket, and a client already mid-pull needs no second wake.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

struct Socket {
    id: u64,
    client_id: String,
    notify: Arc<Notify>,
}

#[derive(Default)]
pub struct WakeRegistry {
    inner: Mutex<HashMap<String, Vec<Socket>>>,
    next_id: AtomicU64,
}

pub struct Subscription {
    ns: String,
    id: u64,
    notify: Arc<Notify>,
    registry: Arc<WakeRegistry>,
}

impl Subscription {
    // await the next wake for this socket (coalesced).
    pub async fn waked(&self) {
        self.notify.notified().await;
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(sockets) = self.registry.inner.lock().unwrap().get_mut(&self.ns) {
            sockets.retain(|s| s.id != self.id);
        }
    }
}

impl WakeRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn subscribe(self: &Arc<Self>, ns: &str, client_id: &str) -> Subscription {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let notify = Arc::new(Notify::new());
        self.inner
            .lock()
            .unwrap()
            .entry(ns.to_string())
            .or_default()
            .push(Socket {
                id,
                client_id: client_id.to_string(),
                notify: notify.clone(),
            });
        Subscription {
            ns: ns.to_string(),
            id,
            notify,
            registry: self.clone(),
        }
    }

    // wake every connected client in the namespace except the pusher.
    pub fn wake(&self, ns: &str, pusher_client_id: &str) {
        if let Some(sockets) = self.inner.lock().unwrap().get(ns) {
            for s in sockets {
                if s.client_id != pusher_client_id {
                    s.notify.notify_one();
                }
            }
        }
    }

    pub fn connections(&self, ns: &str) -> usize {
        self.inner
            .lock()
            .unwrap()
            .get(ns)
            .map(Vec::len)
            .unwrap_or(0)
    }
}
