use std::{path::Path, sync::Arc};

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::{Error, Result, runtime::QueryContext, storage::NativeTableSnapshot};

#[derive(Debug)]
pub(super) struct VerificationCache {
    expected: Arc<[String]>,
    state: Mutex<CacheState>,
    updates: watch::Sender<u64>,
}

#[derive(Debug)]
struct CacheState {
    verified: Vec<Option<VerifiedSegment>>,
    active: Option<u64>,
    next_leader: u64,
    generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VerifiedSegment {
    expected: String,
    fingerprint: String,
}

enum Claim {
    Hit,
    Wait(watch::Receiver<u64>),
    Lead {
        indices: Vec<usize>,
        guard: LeaderGuard,
    },
}

struct LeaderGuard {
    cache: Arc<VerificationCache>,
    id: u64,
    armed: bool,
}

impl VerificationCache {
    pub(super) fn new(snapshot: &NativeTableSnapshot) -> Arc<Self> {
        let expected = snapshot.segment_verification_keys();
        let verified = expected
            .iter()
            .enumerate()
            .map(|(index, expected)| {
                snapshot
                    .verified_segment_fingerprints()
                    .get(index)
                    .and_then(|fingerprint| fingerprint.as_ref())
                    .map(|fingerprint| VerifiedSegment {
                        expected: expected.clone(),
                        fingerprint: fingerprint.clone(),
                    })
            })
            .collect();
        let (updates, _) = watch::channel(0);
        Arc::new(Self {
            state: Mutex::new(CacheState {
                verified,
                active: None,
                next_leader: 1,
                generation: 0,
            }),
            expected: expected.into(),
            updates,
        })
    }

    pub(super) async fn verify(
        self: &Arc<Self>,
        root: Arc<Path>,
        snapshot: Arc<NativeTableSnapshot>,
        context: Arc<QueryContext>,
    ) -> Result<()> {
        loop {
            context.check_cancelled()?;
            let quick_root = Arc::clone(&root);
            let quick_snapshot = Arc::clone(&snapshot);
            let fingerprints = context
                .spill
                .run_query_io(move |control| {
                    quick_snapshot
                        .segment_file_fingerprints(&quick_root, || control.check_cancelled())
                })
                .await?;
            context.check_cancelled()?;

            match self.claim(&fingerprints)? {
                Claim::Hit => return Ok(()),
                Claim::Wait(mut updates) => {
                    tokio::select! {
                        _ = context.control.cancelled() => return Err(Error::Cancelled),
                        changed = updates.changed() => {
                            if changed.is_err() {
                                return Err(Error::Internal(
                                    "native verification singleflight closed unexpectedly"
                                        .to_owned(),
                                ));
                            }
                        }
                    }
                }
                Claim::Lead { indices, mut guard } => {
                    context
                        .metrics
                        .add_native_full_verification_segments(indices.len());
                    let verify_root = Arc::clone(&root);
                    let verify_snapshot = Arc::clone(&snapshot);
                    let verify_indices = indices.clone();
                    let verified = context
                        .spill
                        .run_query_io(move |control| {
                            verify_snapshot.verify_segment_indices(
                                &verify_root,
                                &verify_indices,
                                || control.check_cancelled(),
                            )
                        })
                        .await?;
                    context.check_cancelled()?;
                    self.publish(guard.id, &indices, verified)?;
                    guard.armed = false;
                    return Ok(());
                }
            }
        }
    }

    fn claim(self: &Arc<Self>, fingerprints: &[Option<String>]) -> Result<Claim> {
        if fingerprints.len() != self.expected.len() {
            return Err(Error::Internal(
                "native verification cache does not match its snapshot".to_owned(),
            ));
        }
        let updates = self.updates.subscribe();
        let mut state = self.state.lock();
        let indices = fingerprints
            .iter()
            .enumerate()
            .filter_map(|(index, fingerprint)| {
                let matches = fingerprint.as_ref().is_some_and(|fingerprint| {
                    state.verified[index].as_ref().is_some_and(|verified| {
                        verified.expected == self.expected[index]
                            && verified.fingerprint == *fingerprint
                    })
                });
                (!matches).then_some(index)
            })
            .collect::<Vec<_>>();
        if indices.is_empty() {
            return Ok(Claim::Hit);
        }
        if state.active.is_some() {
            return Ok(Claim::Wait(updates));
        }
        let id = state.next_leader;
        state.next_leader = state.next_leader.wrapping_add(1).max(1);
        state.active = Some(id);
        drop(state);
        Ok(Claim::Lead {
            indices,
            guard: LeaderGuard {
                cache: Arc::clone(self),
                id,
                armed: true,
            },
        })
    }

    fn publish(
        &self,
        leader: u64,
        indices: &[usize],
        fingerprints: Vec<Option<String>>,
    ) -> Result<()> {
        if indices.len() != fingerprints.len() {
            return Err(Error::Internal(
                "native verification returned an invalid fingerprint count".to_owned(),
            ));
        }
        let mut state = self.state.lock();
        if state.active != Some(leader) {
            return Err(Error::Internal(
                "native verification singleflight lost its leader".to_owned(),
            ));
        }
        for (&index, fingerprint) in indices.iter().zip(fingerprints) {
            state.verified[index] = fingerprint.map(|fingerprint| VerifiedSegment {
                expected: self.expected[index].clone(),
                fingerprint,
            });
        }
        state.active = None;
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        drop(state);
        self.updates.send_replace(generation);
        Ok(())
    }

    fn abandon(&self, leader: u64) {
        let mut state = self.state.lock();
        if state.active != Some(leader) {
            return;
        }
        state.active = None;
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        drop(state);
        self.updates.send_replace(generation);
    }
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cache.abandon(self.id);
        }
    }
}
