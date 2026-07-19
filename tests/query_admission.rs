#[path = "../src/http_shell/query/admission.rs"]
mod admission;

use admission::{
    AdmissionController, AdmissionError, AdmissionLayer, AdmissionLimits, AdmissionPrincipal,
    PrincipalAdmissionConfig, ResourceKind, ResourceRequest,
};

fn principal(value: &str) -> AdmissionPrincipal {
    AdmissionPrincipal::new(value).unwrap()
}

fn limits(max_running: usize, max_queued: usize, memory: u64) -> AdmissionLimits {
    AdmissionLimits::new(
        max_running,
        max_queued,
        ResourceRequest::new(memory, memory, memory),
    )
}

fn register(
    controller: &AdmissionController,
    id: &AdmissionPrincipal,
    max_running: usize,
    max_queued: usize,
    memory: u64,
    weight: u32,
) {
    controller
        .register_principal(
            id.clone(),
            PrincipalAdmissionConfig::new(limits(max_running, max_queued, memory))
                .with_weight(weight),
        )
        .unwrap();
}

#[tokio::test]
async fn eight_queries_run_and_ticket_drop_releases_every_charge() {
    let controller = AdmissionController::new(limits(8, 32, 80)).unwrap();
    let analyst = principal("analyst");
    register(&controller, &analyst, 8, 32, 80, 1);
    let request = ResourceRequest::new(10, 2, 3);
    let mut tickets = Vec::new();
    for _ in 0..8 {
        tickets.push(
            controller
                .enqueue(&analyst, request)
                .unwrap()
                .wait()
                .await
                .unwrap(),
        );
    }
    let ninth = controller.enqueue(&analyst, request).unwrap();
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.active, 8);
    assert_eq!(snapshot.queued, 1);
    assert_eq!(snapshot.resources, ResourceRequest::new(80, 16, 24));

    drop(tickets.pop());
    let ninth = ninth.wait().await.unwrap();
    assert_eq!(ninth.principal(), &analyst);
    assert_eq!(ninth.resources(), request);
    assert_eq!(controller.snapshot().active, 8);

    drop(tickets);
    ninth.release();
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.active, 0);
    assert_eq!(snapshot.queued, 0);
    assert_eq!(snapshot.resources, ResourceRequest::default());
}

#[tokio::test]
async fn configured_weights_are_fair_without_admin_special_casing() {
    let controller = AdmissionController::new(limits(8, 128, 8)).unwrap();
    let blocker = principal("blocker");
    let admin_named = principal("admin");
    let query_named = principal("query");
    register(&controller, &blocker, 1, 8, 8, 1);
    register(&controller, &admin_named, 8, 64, 8, 3);
    register(&controller, &query_named, 8, 64, 8, 1);
    let blocker_ticket = controller
        .enqueue(&blocker, ResourceRequest::new(8, 0, 0))
        .unwrap()
        .wait()
        .await
        .unwrap();

    let mut admin_waiters = Vec::new();
    let mut query_waiters = Vec::new();
    for _ in 0..16 {
        admin_waiters.push(
            controller
                .enqueue(&admin_named, ResourceRequest::new(1, 0, 0))
                .unwrap(),
        );
        query_waiters.push(
            controller
                .enqueue(&query_named, ResourceRequest::new(1, 0, 0))
                .unwrap(),
        );
    }
    drop(blocker_ticket);

    let snapshot = controller.snapshot();
    let admin = snapshot
        .principals
        .iter()
        .find(|item| item.principal == admin_named)
        .unwrap();
    let query = snapshot
        .principals
        .iter()
        .find(|item| item.principal == query_named)
        .unwrap();
    assert_eq!((admin.active, query.active), (6, 2));
    assert!(admin.active > 0 && query.active > 0);
    assert_eq!((admin.weight, query.weight), (3, 1));

    drop(admin_waiters);
    drop(query_waiters);
    assert_eq!(controller.snapshot().active, 0);

    let defaults = PrincipalAdmissionConfig::new(limits(1, 1, 1));
    assert_eq!(defaults.weight, 1, "roles do not imply scheduler weight");
}

#[tokio::test]
async fn excessive_requests_and_full_queues_are_rejected_stably() {
    let controller = AdmissionController::new(limits(1, 8, 10)).unwrap();
    let analyst = principal("analyst");
    register(&controller, &analyst, 1, 1, 5, 1);

    for _ in 0..2 {
        assert_eq!(
            controller
                .enqueue(&analyst, ResourceRequest::new(6, 0, 0))
                .unwrap_err(),
            AdmissionError::RequestExceedsLimit {
                layer: AdmissionLayer::Principal,
                resource: ResourceKind::Memory,
            }
        );
    }
    assert_eq!(
        controller
            .enqueue(&analyst, ResourceRequest::new(11, 0, 0))
            .unwrap_err(),
        AdmissionError::RequestExceedsLimit {
            layer: AdmissionLayer::Engine,
            resource: ResourceKind::Memory,
        }
    );

    let active = controller
        .enqueue(&analyst, ResourceRequest::new(1, 0, 0))
        .unwrap()
        .wait()
        .await
        .unwrap();
    let mut queued = controller
        .enqueue(&analyst, ResourceRequest::new(1, 0, 0))
        .unwrap();
    assert_eq!(
        controller
            .enqueue(&analyst, ResourceRequest::new(1, 0, 0))
            .unwrap_err(),
        AdmissionError::QueueFull {
            layer: AdmissionLayer::Principal,
        }
    );
    assert!(queued.cancel());
    assert_eq!(queued.wait().await.unwrap_err(), AdmissionError::Cancelled);
    assert_eq!(controller.snapshot().rejected, 4);
    drop(active);
    assert_eq!(controller.snapshot().active, 0);
}

#[tokio::test]
async fn principal_updates_trim_tail_and_removal_cancels_the_head() {
    let controller = AdmissionController::new(limits(2, 16, 8)).unwrap();
    let blocker = principal("blocker");
    let analyst = principal("analyst");
    register(&controller, &blocker, 1, 4, 8, 1);
    register(&controller, &analyst, 2, 3, 8, 1);
    let blocker = controller
        .enqueue(&blocker, ResourceRequest::new(8, 0, 0))
        .unwrap()
        .wait()
        .await
        .unwrap();
    let first = controller
        .enqueue(&analyst, ResourceRequest::new(1, 0, 0))
        .unwrap();
    let second = controller
        .enqueue(&analyst, ResourceRequest::new(1, 0, 0))
        .unwrap();
    let third = controller
        .enqueue(&analyst, ResourceRequest::new(1, 0, 0))
        .unwrap();

    controller
        .update_principal(&analyst, PrincipalAdmissionConfig::new(limits(1, 1, 8)))
        .unwrap();
    assert_eq!(
        second.wait().await.unwrap_err(),
        AdmissionError::PrincipalUpdated
    );
    assert_eq!(
        third.wait().await.unwrap_err(),
        AdmissionError::PrincipalUpdated
    );
    assert_eq!(controller.snapshot().queued, 1);

    controller.remove_principal(&analyst).unwrap();
    assert_eq!(
        first.wait().await.unwrap_err(),
        AdmissionError::PrincipalRemoved
    );
    assert_eq!(controller.snapshot().queued, 0);
    assert!(matches!(
        controller.enqueue(&analyst, ResourceRequest::default()),
        Err(AdmissionError::UnknownPrincipal(_))
    ));
    drop(blocker);
    assert_eq!(controller.snapshot().active, 0);
}

#[tokio::test]
async fn one_principal_is_strictly_fifo() {
    let controller = AdmissionController::new(limits(1, 8, 8)).unwrap();
    let analyst = principal("fifo");
    register(&controller, &analyst, 1, 8, 8, 1);
    let active = controller
        .enqueue(&analyst, ResourceRequest::new(1, 0, 0))
        .unwrap()
        .wait()
        .await
        .unwrap();
    let first = controller
        .enqueue(&analyst, ResourceRequest::new(2, 0, 0))
        .unwrap();
    let second = controller
        .enqueue(&analyst, ResourceRequest::new(3, 0, 0))
        .unwrap();
    drop(active);

    let first = tokio::time::timeout(std::time::Duration::from_secs(1), first.wait())
        .await
        .expect("the FIFO head must be admitted")
        .unwrap();
    assert_eq!(first.resources().memory_bytes, 2);
    assert_eq!(controller.snapshot().queued, 1);
    drop(first);
    let second = second.wait().await.unwrap();
    assert_eq!(second.resources().memory_bytes, 3);
    drop(second);
    assert_eq!(controller.snapshot().active, 0);
}
