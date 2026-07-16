use std::{sync::Arc, time::Duration};

use dbtool_core::{
    error::Error,
    model::{InputBudget, ReadBudget, SqlExecuteInput, Value},
    port::CapabilityOperation,
    service::{
        safety::StatementKind, ConnectionManager, FlowControl, InputLimiter, SafetyGuard,
        ThrottleConfig,
    },
};
use dbtool_registry::build_registry;

#[tokio::test]
async fn embedded_registry_manager_safety_and_flow_control_share_core_behavior() {
    let registry = Arc::new(build_registry());
    let manager = ConnectionManager::new(Arc::clone(&registry));
    let input_budget = InputBudget::default();
    let params: &[Value] = &[];
    let create_sql = "create table embedded_values (id integer primary key, note text not null)";
    let insert_sql = "insert into embedded_values (id, note) values (1, 'library path')";

    preflight_sql_execute(create_sql, params, input_budget).unwrap();
    preflight_sql_execute(insert_sql, params, input_budget).unwrap();

    let conn = manager.get_or_connect("sqlite::memory:").await.unwrap();
    let conn_again = manager.get_or_connect("sqlite::memory:").await.unwrap();
    assert!(Arc::ptr_eq(&conn, &conn_again));
    assert!(conn.capabilities().sql);
    let operations = conn.operations();
    for operation in [
        CapabilityOperation::SqlExecuteBudgeted,
        CapabilityOperation::SqlQueryBudgeted,
    ] {
        assert!(
            operations.contains(&operation),
            "embedded caller must negotiate {operation:?} before downcasting"
        );
    }

    assert_eq!(
        SafetyGuard::check("select 1", false, None).unwrap(),
        StatementKind::Read
    );
    assert!(matches!(
        SafetyGuard::check("insert into embedded_values (id) values (1)", false, None),
        Err(Error::WriteNotAllowed)
    ));

    let target = "embedded:sqlite::memory:";
    let confirm = match SafetyGuard::check_with_target(create_sql, target, true, None) {
        Err(Error::ConfirmRequired {
            confirm_token,
            impact,
        }) => {
            assert_eq!(impact["op"], "CREATE");
            assert_eq!(impact["target"], target);
            confirm_token
        }
        other => panic!("expected confirm token for embedded create table, got {other:?}"),
    };
    assert_eq!(
        SafetyGuard::check_with_target(create_sql, target, true, Some(&confirm)).unwrap(),
        StatementKind::Destructive
    );

    let sql = conn.as_sql().expect("sqlite connector should expose SQL");
    sql.execute_budgeted(create_sql, params, input_budget)
        .await
        .unwrap();

    assert_eq!(
        SafetyGuard::check(insert_sql, true, None).unwrap(),
        StatementKind::Write
    );
    sql.execute_budgeted(insert_sql, params, input_budget)
        .await
        .unwrap();

    let flow = FlowControl::new(ThrottleConfig {
        max_concurrency: 1,
        acquire_timeout: Duration::from_millis(100),
        request_timeout: Duration::from_secs(1),
        overall_deadline: Some(Duration::from_secs(2)),
        max_retries: 0,
        ..ThrottleConfig::default()
    });
    let rows = flow
        .run_single(sql.query_budgeted(
            "select id, note from embedded_values where id = 1",
            &[],
            ReadBudget::new(100, 8 * 1024 * 1024).unwrap(),
        ))
        .await
        .unwrap();

    assert_eq!(rows.rows.len(), 1);
    assert!(!rows.truncated);
    assert_eq!(rows.rows[0][0], Value::Int(1));
    assert_eq!(rows.rows[0][1], Value::Text("library path".to_owned()));
}

fn preflight_sql_execute(
    sql: &str,
    params: &[Value],
    budget: InputBudget,
) -> dbtool_core::Result<()> {
    let request = SqlExecuteInput { sql, params };
    let limiter = InputLimiter::new(budget, "embedded SQL execute input")?;
    if params.is_empty() {
        limiter.validate_request(&request)?;
    } else {
        limiter.validate_items_with_request(params, &request)?;
    }

    Ok(())
}
