#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidIdentifier(pub String);

impl TryFrom<String> for ValidIdentifier {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        if s.len() > 0 && s.chars().all(|c| c.is_alphanumeric() || c == '_') {
            Ok(ValidIdentifier(s))
        } else {
            Err(anyhow::anyhow!("Invalid identifier: {s:?}"))
        }
    }
}

impl std::fmt::Display for ValidIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::ops::Deref for ValidIdentifier {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub enum WriteAction {
    Insert,
    Update,
}
