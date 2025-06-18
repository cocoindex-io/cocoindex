use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct DatabaseConnectionSpec {
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Settings {
    #[serde(default)]
    pub database: Option<DatabaseConnectionSpec>,
    #[serde(default)]
    #[allow(dead_code)] // Used via serialization/deserialization to Python
    pub app_namespace: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_settings_deserialize_with_database() {
        let json = r#"{
            "database": {
                "url": "postgresql://localhost:5432/test",
                "user": "testuser",
                "password": "testpass"
            },
            "app_namespace": "test_app"
        }"#;

        let settings: Settings = serde_json::from_str(json).unwrap();

        assert!(settings.database.is_some());
        let db = settings.database.unwrap();
        assert_eq!(db.url, "postgresql://localhost:5432/test");
        assert_eq!(db.user, Some("testuser".to_string()));
        assert_eq!(db.password, Some("testpass".to_string()));
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
                "url": "postgresql://localhost:5432/test"
            }
        }"#;

        let settings: Settings = serde_json::from_str(json).unwrap();

        assert!(settings.database.is_some());
        let db = settings.database.unwrap();
        assert_eq!(db.url, "postgresql://localhost:5432/test");
        assert_eq!(db.user, None);
        assert_eq!(db.password, None);
        assert_eq!(settings.app_namespace, "");
    }

    #[test]
    fn test_database_connection_spec_deserialize() {
        let json = r#"{
            "url": "postgresql://localhost:5432/test",
            "user": "testuser",
            "password": "testpass"
        }"#;

        let db_spec: DatabaseConnectionSpec = serde_json::from_str(json).unwrap();

        assert_eq!(db_spec.url, "postgresql://localhost:5432/test");
        assert_eq!(db_spec.user, Some("testuser".to_string()));
        assert_eq!(db_spec.password, Some("testpass".to_string()));
    }
}
