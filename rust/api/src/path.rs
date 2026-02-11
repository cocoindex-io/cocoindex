/// A validated path identifying a component in the pipeline tree.
///
/// # Examples
/// ```
/// use cocoindex::ComponentPath;
///
/// let p: ComponentPath = "setup/table".into();
/// let p: ComponentPath = ("process", "readme.md").into();
///
/// let base: ComponentPath = "transforms".into();
/// let full = base / "embedder";
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComponentPath(String);

impl ComponentPath {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ComponentPath {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for ComponentPath {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<(&str, &str)> for ComponentPath {
    fn from((a, b): (&str, &str)) -> Self {
        Self(format!("{a}/{b}"))
    }
}

impl From<(&str, &String)> for ComponentPath {
    fn from((a, b): (&str, &String)) -> Self {
        Self(format!("{a}/{b}"))
    }
}

impl<S: AsRef<str>> std::ops::Div<S> for ComponentPath {
    type Output = Self;
    fn div(self, rhs: S) -> Self {
        Self(format!("{}/{}", self.0, rhs.as_ref()))
    }
}

impl std::fmt::Display for ComponentPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_str() {
        let p: ComponentPath = "a/b".into();
        assert_eq!(p.as_str(), "a/b");
    }

    #[test]
    fn from_tuple() {
        let p: ComponentPath = ("a", "b").into();
        assert_eq!(p.as_str(), "a/b");
    }

    #[test]
    fn div_operator() {
        let p: ComponentPath = "a".into();
        let p = p / "b";
        assert_eq!(p.as_str(), "a/b");
    }

    #[test]
    fn display() {
        let p: ComponentPath = "x/y/z".into();
        assert_eq!(format!("{p}"), "x/y/z");
    }
}
