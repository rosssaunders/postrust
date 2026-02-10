use crate::parser::ast::CreateFunctionStatement;
use crate::tcop::engine::{EngineError, QueryResult, UserFunction, with_ext_write};

pub async fn execute_create_function(
    create: &CreateFunctionStatement,
) -> Result<QueryResult, EngineError> {
    let uf = UserFunction {
        name: create.name.iter().map(|s| s.to_ascii_lowercase()).collect(),
        params: create.params.clone(),
        return_type: create.return_type.clone(),
        body: create.body.trim().to_string(),
        language: create.language.clone(),
    };
    with_ext_write(|ext| {
        if create.or_replace {
            ext.user_functions.retain(|f| f.name != uf.name);
        } else if ext.user_functions.iter().any(|f| f.name == uf.name) {
            return Err(EngineError {
                message: format!("function \"{}\" already exists", uf.name.join(".")),
            });
        }
        ext.user_functions.push(uf);
        Ok(())
    })?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE FUNCTION".to_string(),
        rows_affected: 0,
    })
}
