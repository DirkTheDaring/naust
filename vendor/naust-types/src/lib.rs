//! Pure OCI registry domain value types (CanonicalRepoName, Digest, RepositoryAccessPattern) for Naust.

pub mod access_pattern;
pub mod canonical_name;
pub mod digest;
pub mod validation;

pub use access_pattern::{AccessPatternError, RepositoryAccessPattern, push_repository_allowed};
pub use canonical_name::{CanonicalRepoName, RepoNameError};
pub use digest::{Digest, DigestParseError};
pub use validation::{is_valid_repo_name, is_valid_tag};
