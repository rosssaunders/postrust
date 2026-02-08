use std::collections::HashMap;

use super::oid::Oid;
use super::table::Table;

#[derive(Debug, Clone)]
pub struct Schema {
    oid: Oid,
    name: String,
    tables: HashMap<String, Table>,
}

impl Schema {
    pub fn new(oid: Oid, name: String) -> Self {
        Self {
            oid,
            name,
            tables: HashMap::new(),
        }
    }

    pub fn oid(&self) -> Oid {
        self.oid
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tables(&self) -> impl Iterator<Item = &Table> {
        self.tables.values()
    }

    pub fn tables_mut(&mut self) -> impl Iterator<Item = &mut Table> {
        self.tables.values_mut()
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    pub fn insert_table(&mut self, table: Table) -> Option<Table> {
        self.tables.insert(table.name().to_string(), table)
    }

    pub fn remove_table(&mut self, name: &str) -> Option<Table> {
        self.tables.remove(name)
    }
}
