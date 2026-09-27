use argon2::password_hash::{SaltString, rand_core::OsRng};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};

#[derive(Debug, thiserror::Error)]
pub enum RobotSecretError {
    #[error("secret must not be empty")]
    Empty,
    #[error("hashing failed")]
    HashFailed,
}

/// Hash a robot secret using Argon2id.
///
/// This is intended for *config-only* storage (hash in TOML, never plaintext).
pub fn hash_robot_secret(secret: &str) -> Result<String, RobotSecretError> {
    let secret = secret.trim_end_matches(['\n', '\r']);
    if secret.is_empty() {
        return Err(RobotSecretError::Empty);
    }

    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2
        .hash_password(secret.as_bytes(), &salt)
        .map(|phc| phc.to_string())
        .map_err(|_| RobotSecretError::HashFailed)
}

/// Sentinel hash used to equalize execution timing during basic auth verification
/// when a requested user or robot does not exist in configuration.
pub const DUMMY_SENTINEL_HASH: &str =
    "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$e6mO8KxX7vN5P9Q2wE4rT";

pub fn verify_robot_secret(secret: &str, secret_hash: &str) -> bool {
    let secret = secret.trim_end_matches(['\n', '\r']);
    if secret.is_empty() || secret_hash.trim().is_empty() {
        return false;
    }

    let Ok(parsed) = PasswordHash::new(secret_hash) else {
        return false;
    };

    Argon2::default()
        .verify_password(secret.as_bytes(), &parsed)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_round_trip() {
        let hash = hash_robot_secret("s3cr3t").expect("hash");
        assert!(verify_robot_secret("s3cr3t", &hash));
        assert!(!verify_robot_secret("wrong", &hash));
    }

    #[test]
    fn sentinel_hash_parses_and_rejects() {
        assert!(!verify_robot_secret("any_password", DUMMY_SENTINEL_HASH));
    }
}
