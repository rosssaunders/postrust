#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupPacket {
    pub user: String,
    pub database: Option<String>,
    pub parameters: Vec<(String, String)>,
}

impl StartupPacket {
    pub fn new(user: impl Into<String>, database: Option<String>) -> Self {
        let user = user.into();
        let mut parameters = vec![("user".to_string(), user.clone())];
        if let Some(db) = &database {
            parameters.push(("database".to_string(), db.clone()));
        }
        Self {
            user,
            database,
            parameters,
        }
    }

    pub fn with_parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.push((key.into(), value.into()));
        self
    }

    pub fn parameter(&self, key: &str) -> Option<&str> {
        self.parameters
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, value)| value.as_str())
    }
}
