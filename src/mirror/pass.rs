//! The sync pass (RFC 0002 §4): watermark, read, diff, pin and
//! copy, commit, boundary, unpin, plus the continuous-mode WAL tail
//! (§4 step 7).
//!
//! The pass syncs the target's watermark `W` directly to the source's
//! latest manifest `L`. `W` is recovered by listing the target's
//! `manifest/` directory, and its boundary is derived from `W`. The
//! only source-side footprint is a pin checkpoint named for the target,
//! held while data objects copy and deleted the moment the commit lands.

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use object_store::{ObjectStoreExt, PutMode, PutOptions};
use slatedb::VersionedManifest;
use slatedb::config::CheckpointOptions;
use slatedb_txn_obj::object_store::ObjectStoreBoundaryObject;
use slatedb_txn_obj::{BoundaryObject, MonotonicId};
use tokio::time::Instant;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::MirrorError;
use super::copier::{Copied, Copier};
use super::layout::{self, Closure, ManifestCache, ManifestObjects, object_path};
use crate::config::ResolvedMirrorTarget;
use crate::services::DatabaseHandle;

/// How many times one sync call restarts its pass (pin lapses, source
/// churn racing GC) before giving up and surfacing an error.
const MAX_RESTARTS: usize = 5;

/// The name of the source pin checkpoint for a target.
pub fn pin_name(target_name: &str) -> String {
    format!("sleet-mirror:{target_name}")
}

/// What one sync pass did.
#[derive(Clone, Copy, Debug, Default)]
pub struct PassOutcome {
    /// The manifest id the target head ended at.
    pub head: u64,
    /// The head manifest's `next_wal_sst_id`, the WAL tail's floor.
    pub next_wal_sst_id: u64,
    /// Whether this pass committed new manifests (false: caught up).
    pub committed: bool,
    /// Manifests written to the target.
    pub manifests_committed: u64,
    /// Data objects copied.
    pub copied: Copied,
}

/// A pass-internal failure: either restart the pass from the watermark
/// or fail the sync call.
enum PassError {
    /// Re-run the whole pass; the world moved underneath it.
    Restart(String),
    /// Surface the error.
    Fatal(MirrorError),
}

impl From<MirrorError> for PassError {
    fn from(e: MirrorError) -> Self {
        PassError::Fatal(e)
    }
}

impl From<object_store::Error> for PassError {
    fn from(e: object_store::Error) -> Self {
        PassError::Fatal(e.into())
    }
}

impl From<slatedb::Error> for PassError {
    fn from(e: slatedb::Error) -> Self {
        PassError::Fatal(e.into())
    }
}

/// Run one sync pass: bring the target's watermark to the source's
/// latest manifest. Restarts internally when the pin lapses or source
/// GC races the diff; every mode and the one-shot run exactly this.
pub async fn sync_pass(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    target_name: &str,
    settings: &ResolvedMirrorTarget,
    rclone: Option<&str>,
) -> Result<PassOutcome, MirrorError> {
    let copier = Copier::new(settings, rclone, source, dest);
    let mut cache = ManifestCache::default();
    let mut last_reason = String::new();
    for attempt in 0..MAX_RESTARTS {
        match pass_once(source, dest, target_name, settings, &copier, &mut cache).await {
            Ok(outcome) => return Ok(outcome),
            Err(PassError::Restart(reason)) => {
                debug!(target = target_name, attempt, "pass restarts: {reason}");
                last_reason = reason;
            }
            Err(PassError::Fatal(e)) => return Err(e),
        }
    }
    Err(MirrorError::Stalled {
        target: target_name.to_string(),
        reason: last_reason,
    })
}

async fn pass_once(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    target_name: &str,
    settings: &ResolvedMirrorTarget,
    copier: &Copier<'_>,
    cache: &mut ManifestCache,
) -> Result<PassOutcome, PassError> {
    // 1. Watermark: the target's latest manifest is the last committed
    // state. Its boundary is derived from this watermark.
    let watermark = dest.admin.read_manifest(None).await?;

    // 2. Read the source's latest manifest.
    let mut head =
        source
            .admin
            .read_manifest(None)
            .await?
            .ok_or_else(|| MirrorError::NotADatabase {
                url: source.url.clone(),
            })?;
    layout::check_source(&source.url, &head)?;
    if let Some(w) = &watermark {
        if w.id() > head.id() {
            return Err(MirrorError::Diverged {
                destination: dest.url.clone(),
                dest_id: w.id(),
                source_id: head.id(),
            }
            .into());
        }
        if w.id() == head.id() {
            advance_target_boundary(dest, head.id()).await?;
            return Ok(PassOutcome {
                head: head.id(),
                next_wal_sst_id: head.next_wal_sst_id(),
                committed: false,
                ..Default::default()
            });
        }
    }

    // 3. Diff the closure of L against W's.
    let seeding = watermark.is_none();
    let mut diff = diff_against_watermark(source, &head, watermark.as_ref(), cache).await?;
    let work = plan_work(copier, dest, &diff, seeding).await?;
    if work.is_empty() {
        // Checkpoint-only change: everything the commit references is
        // protected without a pin (step 3): commit directly.
        return commit(source, dest, &diff, &head, Copied::default()).await;
    }

    // 4. Pin and copy. The checkpoint pins the manifest its own
    // creation commits, so the pass adopts that manifest as L and
    // rediffs; nothing is copied yet, so the slip costs a few manifest
    // reads.
    let mut pin = Pin::create(source, target_name, settings.checkpoint_lifetime).await?;
    head = match cache.read(source, pin.manifest_id).await? {
        Some(m) => m.clone(),
        None => {
            pin.delete(source).await;
            return Err(PassError::Restart(
                "the pin's own manifest already vanished".into(),
            ));
        }
    };
    diff = diff_against_watermark(source, &head, watermark.as_ref(), cache).await?;
    let work = plan_work(copier, dest, &diff, seeding).await?;
    let result = async {
        let copied = copy_under_pin(copier, &work, &mut pin, source).await?;
        // A pass whose pin lapses restarts instead of committing.
        if pin.lapsed() {
            return Err(PassError::Restart("pin lapsed before commit".into()));
        }
        // 5. Commit under the pin.
        commit(source, dest, &diff, &head, copied).await
    }
    .await;
    // 6. Unpin. The deletion writes one more source manifest; the next
    // pass commits it through the pinless path.
    pin.delete(source).await;
    result
}

/// The copier-specific work list for one diff: compacted candidates as
/// the copier plans them, plus HEAD-checked WAL misses.
async fn plan_work(
    copier: &Copier<'_>,
    dest: &DatabaseHandle,
    diff: &Diff,
    seeding: bool,
) -> Result<Vec<String>, PassError> {
    let mut work: Vec<String> = copier
        .plan_compacted(diff.candidates.compacted.iter().cloned().collect(), seeding)
        .await?
        .iter()
        .map(|ulid| layout::compacted_rel(ulid))
        .collect();
    work.extend(
        wal_misses(dest, &diff.candidates.wal)
            .await?
            .iter()
            .map(|&id| layout::wal_rel(id)),
    );
    Ok(work)
}

/// The diff of L's closure against W's (§4 step 3).
struct Diff {
    closure: Closure,
    /// L-closure objects not proven present by W's closure.
    candidates: ManifestObjects,
    /// Closure manifests already present at the target: W itself and
    /// the manifests pinned by checkpoints live in both W and L.
    present_manifests: BTreeSet<u64>,
}

async fn diff_against_watermark(
    source: &DatabaseHandle,
    head: &VersionedManifest,
    watermark: Option<&VersionedManifest>,
    cache: &mut ManifestCache,
) -> Result<Diff, PassError> {
    let now = Utc::now();
    let closure = match layout::closure(source, head, now, cache).await? {
        Some(closure) => closure,
        None => {
            return Err(PassError::Restart(
                "a live checkpoint's pinned manifest vanished mid-diff".into(),
            ));
        }
    };
    let mut proven = ManifestObjects::default();
    let mut present_manifests = BTreeSet::new();
    if let Some(w) = watermark {
        // W's own data objects are fully present (completeness) and
        // kept by prune, so shared objects need no check and no copy.
        proven.extend(&layout::manifest_objects(w));
        present_manifests.insert(w.id());
        // A checkpoint live in both pins the same immutable manifest
        // already fetched for L; one live only in W cannot contribute
        // to L's closure.
        let live_in_head: BTreeSet<Uuid> = head
            .checkpoints()
            .iter()
            .filter(|cp| layout::checkpoint_live(cp, now))
            .map(|cp| cp.id)
            .collect();
        for cp in w.checkpoints() {
            if !layout::checkpoint_live(cp, now) || !live_in_head.contains(&cp.id) {
                continue;
            }
            if let Some(objects) = closure.manifests.get(&cp.manifest_id) {
                proven.extend(objects);
                present_manifests.insert(cp.manifest_id);
            }
        }
    }
    let candidates = closure.objects().difference(&proven);
    Ok(Diff {
        closure,
        candidates,
        present_manifests,
    })
}

/// HEAD-check WAL candidates at the target: the tail usually copied
/// them already, and WALs above W are never pruned, so a hit is final.
async fn wal_misses(dest: &DatabaseHandle, wal: &BTreeSet<u64>) -> Result<Vec<u64>, MirrorError> {
    let mut misses =
        layout::head_misses(dest, wal.iter().copied(), |id| layout::wal_rel(*id)).await?;
    misses.sort_unstable();
    Ok(misses)
}

/// Copy the candidate list while refreshing the pin at half-life.
async fn copy_under_pin(
    copier: &Copier<'_>,
    work: &[String],
    pin: &mut Pin,
    source: &DatabaseHandle,
) -> Result<Copied, PassError> {
    let copy = copier.copy(work);
    tokio::pin!(copy);
    loop {
        let refresh_at = pin.refresh_due;
        tokio::select! {
            result = &mut copy => {
                return result.map_err(|e| match e {
                    // An object vanished mid-copy: the pin lapsed or the
                    // world moved; re-run the pass against fresh state.
                    MirrorError::Store(object_store::Error::NotFound { path, .. }) => {
                        PassError::Restart(format!("{path} vanished mid-copy"))
                    }
                    other => PassError::Fatal(other),
                });
            }
            _ = tokio::time::sleep_until(refresh_at) => {
                pin.refresh(source).await?;
            }
        }
    }
}

/// Commit the closure's manifests in ascending id order, `L` last, each
/// with create-if-absent (§4 step 5).
async fn commit(
    source: &DatabaseHandle,
    dest: &DatabaseHandle,
    diff: &Diff,
    head: &VersionedManifest,
    copied: Copied,
) -> Result<PassOutcome, PassError> {
    let mut committed = 0;
    for &id in diff.closure.manifests.keys() {
        if diff.present_manifests.contains(&id) {
            continue;
        }
        let bytes = match source
            .store
            .get(&object_path(source, &layout::manifest_rel(id)))
            .await
        {
            Ok(get) => get.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(PassError::Restart(format!(
                    "source manifest {id} vanished before commit"
                )));
            }
            Err(e) => return Err(e.into()),
        };
        let to = object_path(dest, &layout::manifest_rel(id));
        match dest
            .store
            .put_opts(&to, bytes.clone().into(), PutOptions::from(PutMode::Create))
            .await
        {
            Ok(_) => committed += 1,
            // Create-if-absent found the id taken. A racing mirror
            // task landing the same immutable manifest is success; a
            // different body means another writer forked the target's
            // history, which wedges the pass loudly.
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = dest.store.get(&to).await?.bytes().await?;
                if existing != bytes {
                    return Err(MirrorError::Diverged {
                        destination: dest.url.clone(),
                        dest_id: id,
                        source_id: head.id(),
                    }
                    .into());
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
    advance_target_boundary(dest, head.id()).await?;
    info!(
        head = head.id(),
        manifests = committed,
        objects = copied.objects,
        "mirror pass committed"
    );
    Ok(PassOutcome {
        head: head.id(),
        next_wal_sst_id: head.next_wal_sst_id(),
        committed: true,
        manifests_committed: committed,
        copied,
    })
}

/// Advance the target's manifest boundary through every id below its
/// committed head. SlateDB's checked latest read rejects a cached id at
/// or below this boundary, invalidates its cache, and falls back to a
/// LIST that crosses the mirror's manifest gaps.
///
/// This runs only after the head and its closure have committed. A
/// caught-up pass repeats it to repair a crash between the final
/// manifest PUT and this monotonic CAS.
async fn advance_target_boundary(dest: &DatabaseHandle, head: u64) -> Result<(), MirrorError> {
    let Some(boundary) = head.checked_sub(1) else {
        return Ok(());
    };
    if let Some(root) = local_root(&dest.url) {
        advance_local_boundary(root, boundary).await?;
        debug!(head, boundary, "advanced local mirror manifest boundary");
        return Ok(());
    }
    let object = ObjectStoreBoundaryObject::new(&dest.path, dest.store.clone(), "manifest");
    object.advance(MonotonicId::new(boundary)).await?;
    debug!(head, boundary, "advanced mirror manifest boundary");
    Ok(())
}

/// Resolve a `file://` database URL to its local root. Other stores use
/// SlateDB's object-store boundary CAS.
fn local_root(url: &str) -> Option<PathBuf> {
    let url = url::Url::parse(url).ok()?;
    (url.scheme() == "file").then(|| url.to_file_path().ok())?
}

/// Advance a local target's boundary under an OS file lock. The
/// `object_store` local backend does not implement conditional updates,
/// so this provides the same cross-process monotonicity. The boundary
/// body is replaced from an adjacent synced temporary file, keeping
/// lock-free SlateDB readers from observing a partial integer.
async fn advance_local_boundary(root: PathBuf, boundary: u64) -> Result<(), MirrorError> {
    let boundary_path = root.join("gc").join("manifest.boundary");
    let error_path = boundary_path.clone();
    let result =
        tokio::task::spawn_blocking(move || advance_local_boundary_blocking(&root, boundary))
            .await
            .map_err(|error| MirrorError::LocalBoundary {
                path: error_path.clone(),
                source: std::io::Error::other(format!("boundary task failed: {error}")),
            })?;
    result.map_err(|source| MirrorError::LocalBoundary {
        path: error_path,
        source,
    })
}

fn advance_local_boundary_blocking(root: &Path, boundary: u64) -> std::io::Result<()> {
    let gc = root.join("gc");
    std::fs::create_dir_all(&gc)?;
    let lock_path = gc.join("manifest.boundary.lock");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    lock.lock()?;

    let boundary_path = gc.join("manifest.boundary");
    let current = match std::fs::read_to_string(&boundary_path) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error),
    };
    if current >= boundary {
        return Ok(());
    }

    let temporary = gc.join(format!(".manifest.boundary.{}.tmp", Uuid::new_v4()));
    let write = (|| {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        write!(file, "{boundary}")?;
        file.sync_all()?;
        drop(file);
        match std::fs::rename(&temporary, &boundary_path) {
            Ok(()) => Ok(()),
            // Windows does not replace an existing file with rename.
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                std::fs::remove_file(&boundary_path)?;
                std::fs::rename(&temporary, &boundary_path)
            }
            Err(error) => Err(error),
        }
    })();
    if write.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    write
}

/// The source pin checkpoint one pass holds while data objects copy.
struct Pin {
    id: Uuid,
    manifest_id: u64,
    lifetime: Duration,
    refresh_due: Instant,
    deadline: Instant,
}

impl Pin {
    /// Create the pin checkpoint, named for the target.
    async fn create(
        source: &DatabaseHandle,
        target_name: &str,
        lifetime: Duration,
    ) -> Result<Pin, MirrorError> {
        let result = source
            .admin
            .create_detached_checkpoint(&CheckpointOptions {
                lifetime: Some(lifetime),
                source: None,
                name: Some(pin_name(target_name)),
            })
            .await?;
        let now = Instant::now();
        Ok(Pin {
            id: result.id,
            manifest_id: result.manifest_id,
            lifetime,
            refresh_due: now + lifetime / 2,
            deadline: now + lifetime,
        })
    }

    /// Refresh the pin at half-life. Any failure restarts the pass: a
    /// pin that cannot be proven alive must not back a commit.
    async fn refresh(&mut self, source: &DatabaseHandle) -> Result<(), PassError> {
        match source
            .admin
            .refresh_checkpoint(self.id, Some(self.lifetime))
            .await
        {
            Ok(()) => {
                let now = Instant::now();
                self.refresh_due = now + self.lifetime / 2;
                self.deadline = now + self.lifetime;
                Ok(())
            }
            Err(e) => Err(PassError::Restart(format!("pin refresh failed: {e}"))),
        }
    }

    /// Whether the pin's last confirmed lifetime has run out.
    fn lapsed(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// Delete the pin. Best effort: an expired leftover is stripped by
    /// source GC and only costs the next pass a restart.
    async fn delete(self, source: &DatabaseHandle) {
        if let Err(e) = source.admin.delete_checkpoint(self.id).await {
            warn!(checkpoint = %self.id, "failed to delete pin checkpoint: {e}");
        }
    }
}

/// The WAL tail (§4 step 7): copy WAL SSTs in ascending id order as
/// they appear at the source. Ids are dense, so the poll is one GET of
/// the next expected id, nearly free when idle.
pub struct Tail {
    /// The next WAL id to probe for.
    pub next: u64,
}

impl Tail {
    /// Start a tail at the target's current WAL head: one LIST of the
    /// target's `wal/`, floored by the last pass's `next_wal_sst_id`.
    pub async fn start(dest: &DatabaseHandle, floor: u64) -> Result<Tail, MirrorError> {
        let max = layout::list_wals(dest).await?.last().map(|(id, _)| *id);
        Ok(Tail {
            next: max.map_or(floor, |m| (m + 1).max(floor)),
        })
    }

    /// Raise the floor after a pass (its head's `next_wal_sst_id`).
    pub fn advance_floor(&mut self, floor: u64) {
        self.next = self.next.max(floor);
    }

    /// Copy every WAL SST available at the source, in id order. Returns
    /// the number copied; zero means the tail is caught up.
    pub async fn step(
        &mut self,
        source: &DatabaseHandle,
        dest: &DatabaseHandle,
    ) -> Result<u64, MirrorError> {
        let mut copied = 0;
        loop {
            let rel = layout::wal_rel(self.next);
            let get = match source.store.get(&object_path(source, &rel)).await {
                Ok(get) => get,
                Err(object_store::Error::NotFound { .. }) => return Ok(copied),
                Err(e) => return Err(e.into()),
            };
            let bytes = get.bytes().await?;
            dest.store
                .put(&object_path(dest, &rel), bytes.into())
                .await?;
            debug!(wal = self.next, "tailed WAL SST");
            self.next += 1;
            copied += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The file backend has no conditional update, so its OS-locked
    /// fallback must preserve the same monotonic boundary under racing
    /// processes or tasks.
    #[tokio::test(flavor = "multi_thread")]
    async fn local_boundary_updates_are_monotonic() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}", dir.path().display());
        let mut tasks = Vec::new();
        for head in [8, 64, 3, 41, 17, 63, 12, 2] {
            let url = url.clone();
            tasks.push(tokio::spawn(async move {
                let dest = DatabaseHandle::open(&url).unwrap();
                advance_target_boundary(&dest, head).await.unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let value = std::fs::read_to_string(dir.path().join("gc/manifest.boundary")).unwrap();
        assert_eq!(value, "63");
    }
}
