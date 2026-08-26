use std::fmt;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RepoNameError {
    #[error("repository name is empty")]
    Empty,
    #[error("repository name exceeds maximum length of 255 bytes")]
    TooLong,
    #[error("repository name contains invalid character: {0}")]
    InvalidCharacter(char),
    #[error("repository name component contains path traversal or invalid segment: {0}")]
    InvalidSegment(String),
    #[error("repository name cannot start or end with a slash")]
    LeadingOrTrailingSlash,
    #[error("repository name cannot contain consecutive slashes")]
    ConsecutiveSlashes,
}

/// A validated, canonical repository name matching OCI Distribution Spec:
/// `[a-z0-9]+([._-][a-z0-9]+)*(/[a-z0-9]+([._-][a-z0-9]+)*)*`
///
/// Guaranteed properties:
/// 1. Only lowercase ASCII alphanumeric characters, dots, underscores, dashes, and single forward slashes.
/// 2. No uppercase, non-ASCII, or percent-encoded characters (`%`).
/// 3. No empty segments, leading slashes, trailing slashes, or double slashes (`//`).
/// 4. No dot segments (`.` or `..`) or backslashes (`\`).
/// 5. Cannot escape a parent directory on the filesystem or collide with internal metadata paths.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct CanonicalRepoName(String);

impl CanonicalRepoName {
    pub const MAX_LEN: usize = 255;

    pub fn parse(s: &str) -> Result<Self, RepoNameError> {
        if s.is_empty() {
            return Err(RepoNameError::Empty);
        }
        if s.len() > Self::MAX_LEN {
            return Err(RepoNameError::TooLong);
        }
        if s.starts_with('/') || s.ends_with('/') {
            return Err(RepoNameError::LeadingOrTrailingSlash);
        }
        if s.contains("//") {
            return Err(RepoNameError::ConsecutiveSlashes);
        }

        // Validate each component segment
        for segment in s.split('/') {
            if segment.is_empty() {
                return Err(RepoNameError::ConsecutiveSlashes);
            }
            if segment == "." || segment == ".." {
                return Err(RepoNameError::InvalidSegment(segment.to_string()));
            }

            // OCI segment rules: start and end with alnum, separated by [._-]
            let bytes = segment.as_bytes();
            let first = bytes[0];
            let last = bytes[bytes.len() - 1];

            if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
                return Err(RepoNameError::InvalidSegment(format!(
                    "segment '{segment}' must start with lowercase alphanumeric"
                )));
            }
            if !last.is_ascii_lowercase() && !last.is_ascii_digit() {
                return Err(RepoNameError::InvalidSegment(format!(
                    "segment '{segment}' must end with lowercase alphanumeric"
                )));
            }

            for &b in bytes {
                let c = b as char;
                if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '.' || c == '_' || c == '-'
                {
                    // Valid character
                } else {
                    return Err(RepoNameError::InvalidCharacter(c));
                }
            }
        }

        Ok(Self(s.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    /// Safely computes the filesystem repository root directory without path traversal.
    pub fn fs_repo_dir(&self, base_root: &Path) -> Result<PathBuf, RepoNameError> {
        let path = base_root.join("repos").join(&self.0);
        // Verify path does not contain any Normal / ParentDir components that could escape
        for component in path.components() {
            if let Component::ParentDir = component {
                return Err(RepoNameError::InvalidSegment("..".to_string()));
            }
        }
        Ok(path)
    }

    /// Safely computes the S3 repository key prefix.
    pub fn s3_repo_prefix(&self, root_prefix: &str) -> String {
        let prefix = root_prefix.trim_matches('/');
        if prefix.is_empty() {
            format!("repos/{}/", self.0)
        } else {
            format!("{prefix}/repos/{}/", self.0)
        }
    }
}

impl Deref for CanonicalRepoName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for CanonicalRepoName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CanonicalRepoName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for CanonicalRepoName {
    type Error = RepoNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl TryFrom<&str> for CanonicalRepoName {
    type Error = RepoNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<CanonicalRepoName> for String {
    fn from(name: CanonicalRepoName) -> Self {
        name.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_canonical_names() {
        let valid = vec![
            "ubuntu",
            "library/ubuntu",
            "my-org/my-app",
            "sub.domain/project/image",
            "a/b/c/d/e",
            "a-1/b_2/c.3",
        ];
        for name in valid {
            let res = CanonicalRepoName::parse(name);
            assert!(res.is_ok(), "Expected valid name: {name}");
            assert_eq!(res.unwrap().as_str(), name);
        }
    }

    #[test]
    fn test_invalid_canonical_names() {
        let invalid = vec![
            "",
            "/leading/slash",
            "trailing/slash/",
            "double//slash",
            "dot/./segment",
            "parent/../segment",
            "Uppercase/repo",
            "unicode/репо",
            "percent%2Fencoded",
            "back\\slash",
            "-start-dash/repo",
            "end-dash-/repo",
            "repo/-start-dash",
        ];
        for name in invalid {
            assert!(
                CanonicalRepoName::parse(name).is_err(),
                "Expected invalid name: {name}"
            );
        }
    }
}
