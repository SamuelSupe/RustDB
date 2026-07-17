use std::time::Instant;

use crate::runtime::{OperatorHandle, QueryContext};

/// Query-local attribution for the major hash-join phases.
///
/// Build and probe are wall-clock phases. Key work is cumulative active time,
/// so it may exceed either wall-clock phase when several probe lanes run at
/// once.
#[derive(Clone, Default)]
pub(super) struct JoinPhaseMetrics {
    build: Option<OperatorHandle>,
    build_input_poll: Option<OperatorHandle>,
    build_permit_wait: Option<OperatorHandle>,
    build_key_eval: Option<OperatorHandle>,
    build_hash_table: Option<OperatorHandle>,
    key_work: Option<OperatorHandle>,
    join_id: Option<u64>,
}

impl JoinPhaseMetrics {
    pub(super) fn register(context: &QueryContext, join_id: Option<u64>) -> Self {
        let Some(join_id) = join_id else {
            return Self::default();
        };
        Self {
            build: Some(
                context
                    .metrics
                    .register_operator("JoinBuild", Some(join_id)),
            ),
            build_input_poll: None,
            build_permit_wait: None,
            build_key_eval: None,
            build_hash_table: None,
            key_work: None,
            join_id: Some(join_id),
        }
    }

    pub(super) fn with_multiplicity_build(mut self, context: &QueryContext) -> Self {
        if let Some(join_id) = self.join_id {
            let build_id = self.build.as_ref().map_or(join_id, OperatorHandle::id);
            self.build_input_poll = Some(
                context
                    .metrics
                    .register_operator("JoinBuildInputPoll", Some(build_id)),
            );
            self.build_permit_wait = Some(
                context
                    .metrics
                    .register_operator("JoinBuildPermitWait", Some(build_id)),
            );
            self.build_key_eval = Some(
                context
                    .metrics
                    .register_operator("JoinBuildKeyEval", Some(build_id)),
            );
            self.build_hash_table = Some(
                context
                    .metrics
                    .register_operator("JoinBuildHashTable", Some(build_id)),
            );
            self.key_work = Some(
                context
                    .metrics
                    .register_operator("JoinKeyWork", Some(join_id)),
            );
        }
        self
    }

    pub(super) fn start_build(&self) -> PhaseTimer {
        PhaseTimer::new(self.build.clone())
    }

    pub(super) fn start_probe(&self, context: &QueryContext) -> PhaseTimer {
        PhaseTimer::new(self.register_phase(context, "JoinProbe"))
    }

    pub(super) fn start_spill(&self, context: &QueryContext) -> PhaseTimer {
        PhaseTimer::new(self.register_phase(context, "JoinSpillExecution"))
    }

    pub(super) fn measure_key_work<T>(&self, work: impl FnOnce() -> T) -> T {
        measure(&self.key_work, None, work)
    }

    pub(super) fn record_build_input_poll(&self, elapsed: std::time::Duration) {
        record(&self.build_input_poll, elapsed);
    }

    pub(super) fn record_build_permit_wait(&self, elapsed: std::time::Duration) {
        if let Some(operator) = &self.build_permit_wait {
            operator.record_wait(elapsed);
        }
    }

    pub(super) fn measure_build_key_eval<T>(&self, work: impl FnOnce() -> T) -> T {
        measure(&self.build_key_eval, self.key_work.as_ref(), work)
    }

    pub(super) fn measure_build_hash_table<T>(&self, work: impl FnOnce() -> T) -> T {
        measure(&self.build_hash_table, self.key_work.as_ref(), work)
    }

    fn register_phase(&self, context: &QueryContext, name: &'static str) -> Option<OperatorHandle> {
        self.join_id
            .map(|join_id| context.metrics.register_operator(name, Some(join_id)))
    }
}

fn measure<T>(
    operator: &Option<OperatorHandle>,
    aggregate: Option<&OperatorHandle>,
    work: impl FnOnce() -> T,
) -> T {
    let started = Instant::now();
    let result = work();
    let elapsed = started.elapsed();
    record(operator, elapsed);
    if let Some(aggregate) = aggregate {
        aggregate.record_elapsed(elapsed);
    }
    result
}

fn record(operator: &Option<OperatorHandle>, elapsed: std::time::Duration) {
    if let Some(operator) = operator {
        operator.record_elapsed(elapsed);
    }
}

pub(super) struct PhaseTimer {
    operator: Option<OperatorHandle>,
    started: Instant,
}

impl PhaseTimer {
    fn new(operator: Option<OperatorHandle>) -> Self {
        Self {
            operator,
            started: Instant::now(),
        }
    }
}

impl Drop for PhaseTimer {
    fn drop(&mut self) {
        if let Some(operator) = &self.operator {
            operator.finish(self.started.elapsed());
        }
    }
}
