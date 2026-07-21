use std::path::PathBuf;

use rustdb::{
    Error, Result,
    http_shell::{
        AdminCommand, ServiceStateReport, check_service_state, repair_service_state,
        security::{SecurityState, default_state_root},
        send_admin_command,
    },
};

use super::operations::{ServiceAdminArgs, ServiceOperation, ServiceStateArgs};

pub(super) async fn run(command: &ServiceOperation) -> Result<()> {
    match command {
        ServiceOperation::Check { paths, json } => {
            let report = check_service_state(
                &paths.database,
                state_root(paths)?,
                paths.result_directory.as_deref(),
            )?;
            print_report(&report, *json)?;
            require_healthy(&report)
        }
        ServiceOperation::Repair { paths, apply, json } => {
            let report = repair_service_state(
                &paths.database,
                state_root(paths)?,
                paths.result_directory.as_deref(),
                *apply,
            )?;
            print_report(&report, *json)?;
            require_healthy(&report)
        }
        ServiceOperation::Status { server } => send(server, AdminCommand::Status {}).await,
        ServiceOperation::ReloadTokens { server } => {
            send(server, AdminCommand::ReloadTokens {}).await
        }
        ServiceOperation::RotateToken { server, principal } => {
            send(
                server,
                AdminCommand::RotateToken {
                    principal: principal.clone(),
                },
            )
            .await
        }
        ServiceOperation::RevokeToken { server, token_id } => {
            send(
                server,
                AdminCommand::RevokeToken {
                    token_id: token_id.clone(),
                },
            )
            .await
        }
        ServiceOperation::Shutdown { server } => send(server, AdminCommand::Shutdown {}).await,
    }
}

async fn send(server: &ServiceAdminArgs, command: AdminCommand) -> Result<()> {
    let response = send_admin_command(admin_socket(server)?, command).await?;
    if !response.ok {
        return Err(Error::Execution(
            response
                .error
                .unwrap_or_else(|| "local admin command failed".to_owned()),
        ));
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&response.data.unwrap_or(serde_json::Value::Null)).map_err(
            |error| Error::Internal(format!("failed to encode admin response: {error}"))
        )?
    );
    Ok(())
}

fn admin_socket(server: &ServiceAdminArgs) -> Result<PathBuf> {
    if let Some(path) = &server.admin_socket {
        return Ok(path.clone());
    }
    let root = server
        .state_root
        .clone()
        .map(Ok)
        .unwrap_or_else(default_state_root)?;
    Ok(SecurityState::locate_for_native_database(root, &server.database)?.admin_socket_path())
}

fn state_root(paths: &ServiceStateArgs) -> Result<PathBuf> {
    paths
        .state_root
        .clone()
        .map(Ok)
        .unwrap_or_else(default_state_root)
}

fn print_report(report: &ServiceStateReport, json: bool) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).map_err(|error| {
                Error::Internal(format!("failed to encode service state report: {error}"))
            })?
        );
    } else {
        println!(
            "service state: {} (queries={}, results={}, repaired={})",
            if report.healthy {
                "healthy"
            } else {
                "unhealthy"
            },
            report.checked_queries,
            report.checked_results,
            report.repaired_actions,
        );
        for issue in &report.issues {
            println!("error [{}]: {}", issue.component, issue.message);
        }
    }
    Ok(())
}

fn require_healthy(report: &ServiceStateReport) -> Result<()> {
    if report.healthy {
        Ok(())
    } else {
        Err(Error::Execution(
            "HTTP service state integrity check failed".into(),
        ))
    }
}
