use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use tokio::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify, broadcast};

use crate::storage::entity::Entity;

pub(crate) mod entity;

const CHANNEL_SIZE: usize = 1024;

#[derive(Debug)]
struct Entry {
    data: Entity,
    expires_at: Option<Instant>,
}

pub(crate) struct DbDropGuard {
    db: Db,
}

impl DbDropGuard {
    pub(crate) fn new() -> Self {
        Self { db: Db::new() }
    }

    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(self.db.shutdown_purge_task());
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Db {
    shared: Arc<Shared>,
}

impl Db {
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entities: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    pub(crate) async fn get(&self, key: &Entity) -> Option<Entity> {
        let state = self.shared.state.lock().await;
        state.entities.get(key).map(|entry| entry.data.clone())
    }

    pub(crate) async fn del(&self, key: &Entity) -> Option<Entity> {
        let mut state = self.shared.state.lock().await;
        state.entities.remove(key).map(|entry| entry.data.clone())
    }

    pub(crate) async fn set(&self, key: Entity, value: Entity, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().await;
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            let when = Instant::now() + duration;
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);
            when
        });

        let prev = state.entities.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, key.clone()));
            }
        }

        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        drop(state);
        if notify {
            self.shared.background_task.notify_one();
        }
    }

    pub(crate) async fn subscribe(&self, key: String) -> broadcast::Receiver<Entity> {
        use std::collections::hash_map::Entry;

        let mut state = self.shared.state.lock().await;

        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                let (tx, rx) = broadcast::channel(CHANNEL_SIZE);
                e.insert(tx);
                rx
            }
        }
    }

    pub(crate) async fn publish(&self, key: &str, value: Entity) -> usize {
        let state = self.shared.state.lock().await;

        state
            .pub_sub
            .get(key)
            .map(|tx| tx.send(value).unwrap_or(0))
            .unwrap_or(0)
    }

    pub(crate) async fn shutdown_purge_task(&self) {
        let mut state = self.shared.state.lock().await;
        state.shutdown = true;
        drop(state);
        self.shared.background_task.notify_one();
    }
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    background_task: Notify,
}

impl Shared {
    async fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().await;
        if state.shutdown {
            return None;
        }
        let state = &mut *state;
        let now = Instant::now();
        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }
            state.entities.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }
        None
    }

    async fn is_shutdown(&self) -> bool {
        self.state.lock().await.shutdown
    }
}

#[derive(Debug)]
struct State {
    entities: HashMap<Entity, Entry>,
    pub_sub: HashMap<String, broadcast::Sender<Entity>>,
    expirations: BTreeSet<(Instant, Entity)>,
    shutdown: bool,
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown().await {
        if let Some(when) = shared.purge_expired_keys().await {
            tokio::select! {
                _ = tokio::time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            shared.background_task.notified().await;
        }
    }
}
