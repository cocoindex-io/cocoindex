use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct DatabaseConnectionSpec {
    pub uri: String,
    pub user: Option<String>,
    pub password: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Settings {
    pub database: DatabaseConnectionSpec,
}
