pub enum WriteAction {
    Insert,
    Update,
}

pub fn sanitize_identifier(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        if c.is_alphanumeric() || c == '_' {
            result.push(c);
        } else {
            result.push_str("__");
        }
    }
    result
}
