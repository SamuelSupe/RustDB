use std::{sync::OnceLock, time::Instant};

use tokio::sync::Barrier;

pub(crate) struct QueryStartGate {
    ready: Barrier,
    start: Barrier,
    origin: OnceLock<Instant>,
}

impl QueryStartGate {
    pub(crate) fn try_new(queries: usize) -> Result<Self, String> {
        let participants = queries
            .checked_add(1)
            .ok_or_else(|| "query start gate participant count overflowed".to_owned())?;
        Ok(Self {
            ready: Barrier::new(participants),
            start: Barrier::new(participants),
            origin: OnceLock::new(),
        })
    }

    pub(crate) async fn wait_until_ready(&self) {
        self.ready.wait().await;
    }

    pub(crate) async fn release(&self) -> Result<Instant, String> {
        let origin = Instant::now();
        self.origin
            .set(origin)
            .map_err(|_| "query start gate was released more than once".to_owned())?;
        self.start.wait().await;
        Ok(origin)
    }

    pub(crate) async fn wait_for_start(&self) -> Result<(Instant, Instant), String> {
        self.ready.wait().await;
        self.start.wait().await;
        let origin = self
            .origin
            .get()
            .copied()
            .ok_or_else(|| "query start gate has no release timestamp".to_owned())?;
        Ok((origin, Instant::now()))
    }
}

#[cfg(test)]
mod tests {
    use super::QueryStartGate;
    use std::sync::Arc;

    #[test]
    fn releases_four_ready_queries_from_one_origin() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let gate = Arc::new(QueryStartGate::try_new(4).unwrap());
            let tasks = (0..4)
                .map(|slot| {
                    let gate = Arc::clone(&gate);
                    tokio::spawn(async move {
                        let (origin, started) = gate.wait_for_start().await.unwrap();
                        (slot, origin, started)
                    })
                })
                .collect::<Vec<_>>();

            gate.wait_until_ready().await;
            let origin = gate.release().await.unwrap();
            let mut slots = Vec::new();
            for task in tasks {
                let (slot, observed_origin, started) = task.await.unwrap();
                assert_eq!(observed_origin, origin);
                assert!(started >= origin);
                slots.push(slot);
            }
            slots.sort_unstable();
            assert_eq!(slots, vec![0, 1, 2, 3]);
        });
    }
}
