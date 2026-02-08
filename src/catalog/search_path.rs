#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchPath {
    schemas: Vec<String>,
}

impl Default for SearchPath {
    fn default() -> Self {
        Self::new(["pg_catalog", "public"])
    }
}

impl SearchPath {
    pub fn new<I, S>(schemas: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut normalized = Vec::new();
        let mut seen_pg_catalog = false;

        for schema in schemas {
            let schema = schema.into().trim().to_ascii_lowercase();
            if schema.is_empty() {
                continue;
            }
            if schema == "pg_catalog" {
                seen_pg_catalog = true;
            }
            if !normalized.iter().any(|existing| existing == &schema) {
                normalized.push(schema);
            }
        }

        if !seen_pg_catalog {
            normalized.insert(0, "pg_catalog".to_string());
        }

        Self {
            schemas: normalized,
        }
    }

    pub fn schemas(&self) -> &[String] {
        &self.schemas
    }
}
