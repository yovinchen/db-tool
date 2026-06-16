use std::{sync::Arc, time::Duration};

use dbtool_core::{
    error::Error,
    model::Value,
    service::{safety::StatementKind, ConnectionManager, FlowControl, SafetyGuard, ThrottleConfig},
};
use dbtool_registry::build_registry;

#[tokio::test]
async fn embedded_registry_manager_safety_and_flow_control_share_core_behavior() {
    let registry = Arc::new(build_registry());
    let manager = ConnectionManager::new(Arc::clone(&registry));

    let conn = manager.get_or_connect("sqlite::memory:").await.unwrap();
    let conn_again = manager.get_or_connect("sqlite::memory:").await.unwrap();
    assert!(Arc::ptr_eq(&conn, &conn_again));
    assert!(conn.capabilities().sql);

    assert_eq!(
        SafetyGuard::check("select 1", false, None).unwrap(),
        StatementKind::Read
    );
    assert!(matches!(
        SafetyGuard::check("insert into embedded_values (id) values (1)", false, None),
        Err(Error::WriteNotAllowed)
    ));

    let target = "embedded:sqlite::memory:";
    let create_sql = "create table embedded_values (id integer primary key, note text not null)";
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
    sql.execute(create_sql, &[]).await.unwrap();

    assert_eq!(
        SafetyGuard::check(
            "insert into embedded_values (id, note) values (1, 'ok')",
            true,
            None
        )
        .unwrap(),
        StatementKind::Write
    );
    sql.execute(
        "insert into embedded_values (id, note) values (1, 'library path')",
        &[],
    )
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
        .run_single(sql.query("select id, note from embedded_values where id = 1", &[]))
        .await
        .unwrap();

    assert_eq!(rows.rows.len(), 1);
    assert_eq!(rows.rows[0][0], Value::Int(1));
    assert_eq!(rows.rows[0][1], Value::Text("library path".to_owned()));
}
