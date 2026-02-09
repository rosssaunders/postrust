use postgrust::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

#[derive(Debug)]
struct Fixture {
    sql: &'static str,
    expected_rows: Vec<Vec<&'static str>>,
}

fn run_fixture(sql: &str) -> Vec<Vec<String>> {
    let mut session = PostgresSession::new();
    let out = session.run_sync([FrontendMessage::Query {
        sql: sql.to_string(),
    }]);
    assert!(
        !out.iter()
            .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. })),
        "fixture returned an error: {out:?}"
    );
    out.iter()
        .filter_map(|msg| match msg {
            BackendMessage::DataRow { values } => Some(values.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn differential_fixture_suite() {
    let fixtures = vec![
        Fixture {
            sql: "SELECT 1 + 2",
            expected_rows: vec![vec!["3"]],
        },
        Fixture {
            sql: "SELECT id FROM (SELECT 2 AS id UNION SELECT 1 AS id) t ORDER BY 1",
            expected_rows: vec![vec!["1"], vec!["2"]],
        },
        Fixture {
            sql: "CREATE TABLE t_diff (id int8, owner text); INSERT INTO t_diff VALUES (1, 'a'), (2, 'b'); SELECT owner FROM t_diff WHERE id = 2",
            expected_rows: vec![vec!["b"]],
        },
    ];

    for fixture in fixtures {
        let rows = run_fixture(fixture.sql);
        let expected = fixture
            .expected_rows
            .iter()
            .map(|row| row.iter().map(|v| v.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(rows, expected, "fixture SQL: {}", fixture.sql);
    }
}
