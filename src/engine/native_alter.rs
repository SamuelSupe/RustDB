use std::{sync::Arc, time::Duration};

use arrow::datatypes::SchemaRef;
use sqlparser::ast::Statement;
use tokio::sync::OwnedSemaphorePermit;

use super::{QueryResult, Session};
use crate::{
    Error, Result,
    command::{NativeAlterCommand, NativeAlterOperation, NativeWriteCommand, NativeWriteKind},
};

impl Session {
    pub(super) async fn execute_native_alter(
        &self,
        command: NativeAlterCommand,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        if self.engine.inner.database.is_none() {
            return Err(Error::Unsupported(
                "native ALTER TABLE requires Engine::open(path, config)".to_owned(),
            ));
        }
        let transaction = self.native_transaction.clone();
        let catalog = self.pin_catalog()?;
        if catalog.local_table(&command.name).is_some()
            && !transaction
                .as_ref()
                .is_some_and(|transaction| transaction.is_staged(&command.name))
        {
            return Err(Error::Catalog(format!(
                "native ALTER TABLE target '{}' is shadowed by a session-local table or view",
                command.name
            )));
        }
        let snapshot = match transaction.as_ref() {
            Some(transaction) => transaction.working_snapshot(&command.name),
            None => self
                .engine
                .inner
                .database
                .as_ref()
                .and_then(|database| database.table_snapshot(&command.name).ok()),
        };
        let Some(snapshot) = snapshot else {
            if command.if_exists {
                return self.alter_status(permit, admission_wait, parse_time).await;
            }
            return Err(Error::Catalog(format!(
                "native table '{}' does not exist",
                command.name
            )));
        };

        match command.operation {
            NativeAlterOperation::RenameTable { new_name } => {
                let context = self.query_context()?;
                context.metrics.record_query_admission_wait(admission_wait);
                context.metrics.record_sql_parse_time(parse_time);
                self.execute_native_rename_table(
                    &command.name,
                    &new_name,
                    transaction,
                    &catalog,
                    context.clone(),
                )?;
                self.batch_result(crate::command::status("ALTER TABLE")?, permit, context)
            }
            operation => {
                let Some(query) = rewrite_query(&command.name, snapshot.schema(), operation)?
                else {
                    return self.alter_status(permit, admission_wait, parse_time).await;
                };
                self.execute_native_write(
                    NativeWriteCommand {
                        qualifier: crate::catalog_name::full_qualifier(&command.name),
                        name: command.name,
                        query,
                        kind: NativeWriteKind::Alter,
                        returning: None,
                        import: None,
                    },
                    permit,
                    admission_wait,
                    parse_time,
                )
                .await
            }
        }
    }

    fn execute_native_rename_table(
        &self,
        old_name: &str,
        new_name: &str,
        transaction: Option<Arc<super::transaction::TransactionWorkspace>>,
        catalog: &crate::Catalog,
        context: Arc<crate::runtime::QueryContext>,
    ) -> Result<()> {
        if catalog.local_table(new_name).is_some() {
            return Err(Error::Catalog(format!(
                "table or view '{new_name}' already exists in this session"
            )));
        }
        match transaction {
            Some(transaction) => {
                let _mutation = transaction.begin_mutation()?;
                transaction.stage_rename(&self.engine, &self.catalog, old_name, new_name)?;
                context.mark_transaction_mutation_applied();
                Ok(())
            }
            None => {
                let generation = catalog.persistent_generation().unwrap_or(0);
                let engine = self.engine.clone();
                let old_name = old_name.to_owned();
                let new_name = new_name.to_owned();
                self.engine.inner.spill_io.run(move || {
                    engine.ensure_native_healthy()?;
                    let _gate = engine.inner.native_commit.lock();
                    engine.ensure_native_healthy()?;
                    let database = engine.inner.database.as_ref().ok_or_else(|| {
                        Error::Internal("persistent engine lost its native database".to_owned())
                    })?;
                    let commit = database
                        .commit_table_rename(generation, &old_name, &new_name)
                        .map(Some);
                    super::native_write::install_native_catalog_commit(&engine, &context, commit)
                })
            }
        }
    }

    async fn alter_status(
        &self,
        permit: OwnedSemaphorePermit,
        admission_wait: Duration,
        parse_time: Duration,
    ) -> Result<QueryResult> {
        let context = self.query_context()?;
        context.metrics.record_query_admission_wait(admission_wait);
        context.metrics.record_sql_parse_time(parse_time);
        self.batch_result(crate::command::status("ALTER TABLE")?, permit, context)
    }
}

fn rewrite_query(
    table: &str,
    schema: SchemaRef,
    operation: NativeAlterOperation,
) -> Result<Option<Box<sqlparser::ast::Query>>> {
    let mut projections = schema
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()))
        .collect::<Vec<_>>();
    match operation {
        NativeAlterOperation::AddColumn {
            name,
            data_type,
            default,
            if_not_exists,
        } => {
            if field_index(&schema, &name).is_some() {
                return if if_not_exists {
                    Ok(None)
                } else {
                    Err(Error::Catalog(format!("column '{name}' already exists")))
                };
            }
            let value = default
                .map(|value| value.to_string())
                .unwrap_or_else(|| "NULL".to_owned());
            projections.push(format!(
                "CAST(({value}) AS {data_type}) AS {}",
                quote_identifier(&name)
            ));
        }
        NativeAlterOperation::DropColumns { names, if_exists } => {
            let mut dropped = std::collections::HashSet::new();
            for name in names {
                if field_index(&schema, &name).is_none() {
                    if !if_exists {
                        return Err(Error::Catalog(format!("column '{name}' does not exist")));
                    }
                } else {
                    dropped.insert(name);
                }
            }
            if dropped.is_empty() {
                return Ok(None);
            }
            projections = schema
                .fields()
                .iter()
                .filter(|field| !dropped.contains(&field.name().to_ascii_lowercase()))
                .map(|field| quote_identifier(field.name()))
                .collect();
            if projections.is_empty() {
                return Err(Error::Unsupported(
                    "ALTER TABLE cannot drop the final column".to_owned(),
                ));
            }
        }
        NativeAlterOperation::RenameColumn { old_name, new_name } => {
            let index = field_index(&schema, &old_name)
                .ok_or_else(|| Error::Catalog(format!("column '{old_name}' does not exist")))?;
            if field_index(&schema, &new_name).is_some() {
                return Err(Error::Catalog(format!(
                    "column '{new_name}' already exists"
                )));
            }
            projections[index] = format!(
                "{} AS {}",
                quote_identifier(schema.field(index).name()),
                quote_identifier(&new_name)
            );
        }
        NativeAlterOperation::RenameTable { .. } => {
            return Err(Error::Internal(
                "table rename entered the column rewrite path".to_owned(),
            ));
        }
    }
    let sql = format!(
        "SELECT {} FROM {}",
        projections.join(", "),
        crate::catalog_name::quote(table)
    );
    let statements = crate::sql::parse_statements(&sql)?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Err(Error::Internal(
            "generated ALTER TABLE rewrite did not parse as a query".to_owned(),
        ));
    };
    Ok(Some(query.clone()))
}

fn field_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(name))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
