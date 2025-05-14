use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct DatabaseConnectionSpec {
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Settings {
    pub database: DatabaseConnectionSpec,
}
