use postgrust::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

#[test]
fn regression_corpus_smoke() {
    let sql = include_str!("corpus/basic.sql").replace("accounts", "accounts_regression");
    let mut session = PostgresSession::new();
    let out = session.run_sync([FrontendMessage::Query { sql }]);

    assert!(
        !out.iter()
            .any(|msg| matches!(msg, BackendMessage::ErrorResponse { .. })),
        "regression corpus produced an error response: {out:?}"
    );
    assert!(out.iter().any(
        |msg| matches!(msg, BackendMessage::DataRow { values } if values == &vec!["2".to_string()])
    ));
}
