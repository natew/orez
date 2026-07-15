// on-disk retention for per-namespace replica files.
//
// each namespace is one `<name>.sqlite` file (plus its `-wal`/`-shm` sidecars)
// under the host's data_dir. nothing in the sync protocol ever removes them, so
// a consumer that creates many namespaces (one replica per project) grows the
// data dir without bound: soot's `.orez/sync-native` reached gigabytes of
// `proj-*.sqlite` with no ceiling.
//
// deletion is safe only when these replicas are derived and sync-native is the
// sole process allowed to open them. shared or authoritative sqlite files must
// use the disabled policy, even when sync-native's MutateFn is a no-op.
// exclusive retention is therefore an explicit opt-in rather than the default.
//
// this module holds the policy and the pure deletion planner. the Manager
// (namespace.rs) owns the filesystem walk, idle-worker eviction, and the actual
// unlinks, because it also holds the live-namespace map that decides which files
// a worker still has open and must not be touched.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde_json::json;

/// Bounds on how much replica state the host keeps on disk.
///
/// The default is disabled. Use [`RetentionPolicy::exclusive`] only when
/// sync-native owns the files and no other process can open them.
#[derive(Clone, Copy)]
pub struct RetentionPolicy {
    enabled: bool,
    /// Delete a replica whose newest file has not been modified within this
    /// window. An abandoned namespace's replica ages out even under budget.
    max_age: Duration,
    /// Total on-disk budget for all non-live replica files (main + `-wal` +
    /// `-shm`). Once the set exceeds it, the oldest replicas are deleted first
    /// until it fits.
    max_bytes: u64,
    /// Evict a namespace's in-memory worker (thread + open connection) after
    /// this much idle time, so its file is closed and becomes eligible for
    /// deletion. Reopened cheaply on the next request.
    idle_ttl: Duration,
    /// How often the background sweep runs.
    interval: Duration,
}

impl RetentionPolicy {
    /// Retention turned off. No worker eviction or file deletion runs.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            max_age: Duration::ZERO,
            max_bytes: 0,
            idle_ttl: Duration::ZERO,
            interval: Duration::from_secs(600),
        }
    }

    /// Enable retention for derived replicas owned exclusively by sync-native.
    ///
    /// Do not use this for an authoritative database, or for a SQLite database
    /// opened by an application process or any other connection pool. SQLite
    /// cannot safely unlink an open database.
    pub fn exclusive(
        max_age: Duration,
        max_bytes: u64,
        idle_ttl: Duration,
        interval: Duration,
    ) -> Self {
        Self {
            enabled: true,
            max_age,
            max_bytes,
            idle_ttl,
            interval,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn idle_ttl(&self) -> Duration {
        self.idle_ttl
    }

    pub(crate) fn interval(&self) -> Duration {
        self.interval
    }
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

/// One replica's on-disk footprint: the total size of its main file plus any
/// `-wal`/`-shm` sidecars, and the most recent mtime across them.
pub struct ReplicaFile {
    pub mtime: SystemTime,
    pub size: u64,
}

/// Decide which replicas to delete. Deletes anything older than `max_age`, then,
/// if the surviving set still exceeds `max_bytes`, deletes oldest-first until it
/// fits. Returns indices into `files`. Pure: no filesystem or clock access, so
/// the eviction policy is unit-testable in isolation.
pub fn plan_deletions(
    files: &[ReplicaFile],
    policy: &RetentionPolicy,
    now: SystemTime,
) -> Vec<usize> {
    if !policy.enabled {
        return Vec::new();
    }

    let mut delete = vec![false; files.len()];

    // age-based: anything untouched past max_age is abandoned.
    for (i, f) in files.iter().enumerate() {
        let age = now.duration_since(f.mtime).unwrap_or(Duration::ZERO);
        if age > policy.max_age {
            delete[i] = true;
        }
    }

    // size budget over the survivors, oldest-first. ties broken by index so the
    // result is deterministic.
    let mut kept: Vec<usize> = (0..files.len()).filter(|&i| !delete[i]).collect();
    let mut total: u64 = kept.iter().map(|&i| files[i].size).sum();
    if total > policy.max_bytes {
        kept.sort_by(|&a, &b| files[a].mtime.cmp(&files[b].mtime).then(a.cmp(&b)));
        for i in kept {
            if total <= policy.max_bytes {
                break;
            }
            delete[i] = true;
            total -= files[i].size;
        }
    }

    (0..files.len()).filter(|&i| delete[i]).collect()
}

/// What one retention sweep did. Empty when nothing needed eviction or deletion.
#[derive(Default)]
pub struct SweepOutcome {
    pub evicted: usize,
    pub deleted: usize,
    pub bytes_freed: u64,
    errors: Vec<RetentionError>,
}

/// A retention pass stopped because it could not safely inspect, checkpoint,
/// or unlink a replica file.
#[derive(Debug)]
pub struct RetentionError {
    path: PathBuf,
    message: String,
    bytes_freed: u64,
}

impl RetentionError {
    pub(crate) fn new(path: impl AsRef<Path>, error: impl std::fmt::Display) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            message: error.to_string(),
            bytes_freed: 0,
        }
    }

    pub(crate) fn after_unlink(
        path: impl AsRef<Path>,
        error: impl std::fmt::Display,
        bytes_freed: u64,
    ) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            message: error.to_string(),
            bytes_freed,
        }
    }

    pub fn bytes_freed(&self) -> u64 {
        self.bytes_freed
    }

    pub fn emit(&self) {
        eprintln!(
            "{}",
            json!({
                "event": "replica_retention_error",
                "path": self.path.display().to_string(),
                "error": self.message,
                "bytesFreed": self.bytes_freed,
            })
        );
    }
}

impl std::fmt::Display for RetentionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "retention failed for {} after freeing {} bytes: {}",
            self.path.display(),
            self.bytes_freed,
            self.message
        )
    }
}

impl std::error::Error for RetentionError {}

impl SweepOutcome {
    pub fn errors(&self) -> &[RetentionError] {
        &self.errors
    }

    pub(crate) fn record_error(&mut self, error: RetentionError) {
        self.bytes_freed += error.bytes_freed();
        self.errors.push(error);
    }

    pub fn is_empty(&self) -> bool {
        self.evicted == 0 && self.deleted == 0 && self.bytes_freed == 0 && self.errors.is_empty()
    }

    /// Emit a single structured stderr line when the sweep did something, staying
    /// silent otherwise (matching obs.rs's routine-is-silent policy so retention
    /// never floods a consumer's captured log).
    pub fn emit(&self) {
        for error in &self.errors {
            error.emit();
        }
        if self.evicted == 0 && self.deleted == 0 && self.bytes_freed == 0 {
            return;
        }
        eprintln!(
            "{}",
            json!({
                "event": "replica_retention",
                "evicted": self.evicted,
                "deleted": self.deleted,
                "bytesFreed": self.bytes_freed,
            })
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(now: SystemTime, age: Duration) -> SystemTime {
        now - age
    }

    fn policy(max_age: Duration, max_bytes: u64) -> RetentionPolicy {
        RetentionPolicy::exclusive(
            max_age,
            max_bytes,
            Duration::from_secs(1800),
            Duration::from_secs(600),
        )
    }

    #[test]
    fn disabled_never_deletes() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let files = vec![ReplicaFile {
            mtime: at(now, Duration::from_secs(365 * 24 * 3600)),
            size: u64::MAX / 2,
        }];
        let deleted = plan_deletions(&files, &RetentionPolicy::disabled(), now);
        assert!(deleted.is_empty());
    }

    #[test]
    fn default_is_disabled() {
        assert!(!RetentionPolicy::default().is_enabled());
    }

    #[test]
    fn age_deletes_only_stale_files() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let day = Duration::from_secs(24 * 3600);
        let files = vec![
            ReplicaFile {
                mtime: at(now, day),
                size: 10,
            }, // 1d: fresh
            ReplicaFile {
                mtime: at(now, day * 5),
                size: 10,
            }, // 5d: stale
            ReplicaFile {
                mtime: at(now, day * 2),
                size: 10,
            }, // 2d: fresh
        ];
        // budget high enough that only age matters.
        let deleted = plan_deletions(&files, &policy(day * 3, u64::MAX), now);
        assert_eq!(deleted, vec![1]);
    }

    #[test]
    fn budget_evicts_oldest_first_until_under() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let min = Duration::from_secs(60);
        // all young (age never triggers); each 100 bytes; budget 250 -> must drop
        // enough to reach <= 250. total 500, drop the two oldest (200) -> 300,
        // still over, drop the next oldest -> 200 <= 250.
        let files = vec![
            ReplicaFile {
                mtime: at(now, min * 1),
                size: 100,
            }, // newest
            ReplicaFile {
                mtime: at(now, min * 5),
                size: 100,
            }, // oldest
            ReplicaFile {
                mtime: at(now, min * 3),
                size: 100,
            },
            ReplicaFile {
                mtime: at(now, min * 4),
                size: 100,
            },
            ReplicaFile {
                mtime: at(now, min * 2),
                size: 100,
            },
        ];
        let mut deleted = plan_deletions(&files, &policy(Duration::from_secs(3600), 250), now);
        deleted.sort();
        // oldest-first: indices 1 (5m), 3 (4m), 2 (3m) removed -> keeps 200 bytes.
        assert_eq!(deleted, vec![1, 2, 3]);
    }

    #[test]
    fn age_and_budget_compose() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(10_000_000);
        let day = Duration::from_secs(24 * 3600);
        let files = vec![
            ReplicaFile {
                mtime: at(now, day * 10),
                size: 100,
            }, // stale by age
            ReplicaFile {
                mtime: at(now, day * 1),
                size: 100,
            }, // fresh, newest
            ReplicaFile {
                mtime: at(now, day * 2),
                size: 100,
            }, // fresh
        ];
        // age drops index 0; survivors total 200, budget 150 -> drop oldest
        // survivor (index 2, 2d).
        let mut deleted = plan_deletions(&files, &policy(day * 3, 150), now);
        deleted.sort();
        assert_eq!(deleted, vec![0, 2]);
    }

    #[test]
    fn under_budget_and_fresh_keeps_everything() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        let files = vec![
            ReplicaFile {
                mtime: now,
                size: 10,
            },
            ReplicaFile {
                mtime: now,
                size: 10,
            },
        ];
        let deleted = plan_deletions(&files, &policy(Duration::from_secs(3600), 1000), now);
        assert!(deleted.is_empty());
    }
}
