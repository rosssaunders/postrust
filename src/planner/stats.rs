use std::collections::HashMap;

use crate::catalog::{SearchPath, Table, with_catalog_read};
use crate::parser::ast::TableRef;
use crate::storage::heap::with_storage_read;
use crate::storage::tuple::ScalarValue;

use super::PlannerError;

#[derive(Debug, Clone)]
pub struct TableStats {
    pub table: Table,
    pub row_count: usize,
    pub null_fractions: HashMap<String, f64>,
}

impl TableStats {
    pub fn null_fraction(&self, column: &str) -> Option<f64> {
        let key = column.to_ascii_lowercase();
        self.null_fractions.get(&key).copied()
    }

    pub fn index_on_column(&self, column: &str) -> Option<crate::catalog::IndexSpec> {
        let normalized = column.to_ascii_lowercase();
        self.table
            .indexes()
            .iter()
            .find(|index| index.columns.iter().any(|col| col == &normalized))
            .cloned()
    }
}

pub fn table_stats(table: &TableRef) -> Result<TableStats, PlannerError> {
    let table_name = &table.name;
    let resolved = with_catalog_read(|catalog| {
        catalog
            .resolve_table(table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| PlannerError {
        message: err.message,
    })?;

    let rows = with_storage_read(|storage| {
        storage
            .rows_by_table
            .get(&resolved.oid())
            .cloned()
            .unwrap_or_default()
    });

    let mut null_counts = vec![0usize; resolved.columns().len()];
    for row in &rows {
        for (idx, value) in row.iter().enumerate() {
            let value: &ScalarValue = value;
            if matches!(value, ScalarValue::Null) {
                null_counts[idx] += 1;
            }
        }
    }

    let total = rows.len().max(1) as f64;
    let mut null_fractions = HashMap::new();
    for (idx, column) in resolved.columns().iter().enumerate() {
        let fraction = null_counts[idx] as f64 / total;
        null_fractions.insert(column.name().to_string(), fraction);
    }

    Ok(TableStats {
        table: resolved,
        row_count: rows.len(),
        null_fractions,
    })
}
