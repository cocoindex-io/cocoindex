use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct DatabaseConnectionSpec {
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub max_connections: u32,
    pub min_connections: u32,
}

#[derive(Deserialize, Debug, Default)]
pub struct GlobalExecutionOptions {
    pub source_max_inflight_rows: Option<usize>,
    pub source_max_inflight_bytes: Option<usize>,
}

#[derive(Deserialize, Debug, Default)]
pub struct Settings {
    #[serde(default)]
    pub database: Option<DatabaseConnectionSpec>,
    #[serde(default)]
    #[allow(dead_code)] // Used via serialization/deserialization to Python
    pub app_namespace: String,
    #[serde(default)]
    pub global_execution_options: GlobalExecutionOptions,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settings_deserialize_with_database() {
        let json = r#"{
            "database": {
                "url": "postgresql://localhost:5432/test",
                "user": "testuser",
                "password": "testpass",
                "min_connections": 1,
                "max_connections": 10
            },
            "app_namespace": "test_app"
        }"#;

        let settings: Settings = serde_json::from_str(json).unwrap();

        assert!(settings.database.is_some());
        let db = settings.database.unwrap();
        assert_eq!(db.url, "postgresql://localhost:5432/test");
        assert_eq!(db.user, Some("testuser".to_string()));
        assert_eq!(db.password, Some("testpass".to_string()));
        assert_eq!(db.min_connections, 1);
        assert_eq!(db.max_connections, 10);
        assert_eq!(settings.app_namespace, "test_app");
    }

    #[test]
    fn test_settings_deserialize_without_database() {
        let json = r#"{
            "app_namespace": "test_app"
        }"#;

        let settings: Settings = serde_json::from_str(json).unwrap();

        assert!(settings.database.is_none());
        assert_eq!(settings.app_namespace, "test_app");
    }

    #[test]
    fn test_settings_deserialize_empty_object() {
        let json = r#"{}"#;

        let settings: Settings = serde_json::from_str(json).unwrap();

        assert!(settings.database.is_none());
        assert_eq!(settings.app_namespace, "");
    }

    #[test]
    fn test_settings_deserialize_database_without_user_password() {
        let json = r#"{
            "database": {
                "url": "postgresql://localhost:5432/test",
                "min_connections": 1,
                "max_connections": 10
            }
        }"#;

        let settings: Settings = serde_json::from_str(json).unwrap();

        assert!(settings.database.is_some());
        let db = settings.database.unwrap();
        assert_eq!(db.url, "postgresql://localhost:5432/test");
        assert_eq!(db.user, None);
        assert_eq!(db.password, None);
        assert_eq!(db.min_connections, 1);
        assert_eq!(db.max_connections, 10);
        assert_eq!(settings.app_namespace, "");
    }

    #[test]
    fn test_database_connection_spec_deserialize() {
        let json = r#"{
            "url": "postgresql://localhost:5432/test",
            "user": "testuser",
            "password": "testpass",
            "min_connections": 1,
            "max_connections": 10
        }"#;

        let db_spec: DatabaseConnectionSpec = serde_json::from_str(json).unwrap();

        assert_eq!(db_spec.url, "postgresql://localhost:5432/test");
        assert_eq!(db_spec.user, Some("testuser".to_string()));
        assert_eq!(db_spec.password, Some("testpass".to_string()));
        assert_eq!(db_spec.min_connections, 1);
        assert_eq!(db_spec.max_connections, 10);
    }
}
