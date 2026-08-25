/// Pure general-purpose glob pattern matcher.
///
/// Contract:
/// - `*` matches any sequence of zero or more characters.
/// - Character comparisons are exact and case-sensitive.
/// - Slashes (`/`) have no special path-boundary significance; they are matched as ordinary characters.
/// - Does NOT perform security/authorization boundary matching. (For RBAC repository grants, use `crate::rbac`).
pub fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == value;
    }

    let parts: Vec<&str> = pattern.split('*').collect();
    let mut rest = value;

    // 1. Prefix before first '*' must match at start.
    if !parts[0].is_empty() {
        if let Some(r) = rest.strip_prefix(parts[0]) {
            rest = r;
        } else {
            return false;
        }
    }

    // 2. Middle components between '*' must match in sequential order.
    for part in parts.iter().skip(1).take(parts.len().saturating_sub(2)) {
        if part.is_empty() {
            continue;
        }
        if let Some(idx) = rest.find(part) {
            rest = &rest[idx + part.len()..];
        } else {
            return false;
        }
    }

    // 3. Suffix after last '*' must match at end.
    if let Some(last) = parts.last() {
        if last.is_empty() {
            return true;
        }
        return rest.ends_with(last);
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildcard_match_exact() {
        assert!(wildcard_match("foo", "foo"));
        assert!(!wildcard_match("foo", "bar"));
        assert!(!wildcard_match("foo", "foobar"));
        assert!(!wildcard_match("foobar", "foo"));
    }

    #[test]
    fn test_wildcard_match_global_star() {
        assert!(wildcard_match("*", ""));
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("*", "org/repo/app"));
    }

    #[test]
    fn test_wildcard_match_prefix_and_suffix() {
        assert!(wildcard_match("org/*", "org/app"));
        assert!(wildcard_match("org/*", "org/sub/app"));
        assert!(!wildcard_match("org/*", "org"));
        assert!(!wildcard_match("org/*", "other/app"));

        assert!(wildcard_match("*app", "my-app"));
        assert!(wildcard_match("*app", "app"));
        assert!(!wildcard_match("*app", "app-other"));
    }

    #[test]
    fn test_wildcard_match_middle_and_multiple_stars() {
        assert!(wildcard_match("a*c", "abc"));
        assert!(wildcard_match("a*c", "ac"));
        assert!(wildcard_match("a*c", "a123c"));
        assert!(!wildcard_match("a*c", "a123cd"));

        assert!(wildcard_match("a*b*c", "a-b-c"));
        assert!(wildcard_match("a*b*c", "a1b2c"));
        assert!(wildcard_match("a**b", "a-b"));
        assert!(!wildcard_match("a*b*c", "a-c-b"));
    }

    #[test]
    fn test_wildcard_match_empty_and_case() {
        assert!(wildcard_match("", ""));
        assert!(!wildcard_match("", "foo"));
        assert!(!wildcard_match("foo", ""));

        // Case-sensitivity
        assert!(!wildcard_match("Foo", "foo"));
        assert!(!wildcard_match("foo", "FOO"));
    }
}
