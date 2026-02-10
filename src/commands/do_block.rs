use crate::parser::ast::DoStatement;
use crate::tcop::engine::{EngineError, QueryResult};

pub async fn execute_do_block(_do_stmt: &DoStatement) -> Result<QueryResult, EngineError> {
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DO".to_string(),
        rows_affected: 0,
    })
}
