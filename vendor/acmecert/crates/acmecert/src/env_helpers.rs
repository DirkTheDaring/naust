use std::env;

pub fn env_is_one(name: &str) -> bool {
    env::var(name).ok().is_some_and(|v| v == "1")
}

pub fn env_non_empty(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}
