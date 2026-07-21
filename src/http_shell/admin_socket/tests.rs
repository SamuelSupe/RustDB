use std::time::Duration;

use tokio::io::BufReader;

use super::{AdminCommand, AdminResponse, read_bounded_line_with_timeout};

#[test]
fn command_protocol_is_strict_json_lines_payload() {
    let command: AdminCommand =
        serde_json::from_str(r#"{"command":"rotate_token","principal":"alice"}"#).unwrap();
    assert!(matches!(command, AdminCommand::RotateToken { principal } if principal == "alice"));
    assert_eq!(
        serde_json::to_string(&AdminCommand::Status {}).unwrap(),
        r#"{"command":"status"}"#
    );
    for command in ["status", "reload_tokens", "shutdown"] {
        let payload = format!(r#"{{"command":"{command}","unexpected":true}}"#);
        assert!(serde_json::from_str::<AdminCommand>(&payload).is_err());
    }
    let response = AdminResponse::error("failure");
    assert!(!response.ok);
}

#[tokio::test]
async fn beta2_hardening_idle_admin_line_read_times_out() {
    let (_writer, reader) = tokio::io::duplex(64);
    let error = read_bounded_line_with_timeout(
        &mut BufReader::new(reader),
        None,
        Duration::from_millis(10),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("timed out"));
}
