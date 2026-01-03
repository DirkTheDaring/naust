use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Digest {
    hex: String,
}

#[derive(Debug, Error)]
pub enum DigestParseError {
    #[error("unsupported digest algorithm")]
    UnsupportedAlgorithm,

    #[error("invalid digest format")]
    InvalidFormat,

    #[error("invalid hex")]
    InvalidHex,
}

impl Digest {
    pub fn parse(input: &str) -> Result<Self, DigestParseError> {
        // Only sha256 is supported for MVP.
        let (algo, hex) = input
            .split_once(':')
            .ok_or(DigestParseError::InvalidFormat)?;
        if algo != "sha256" {
            return Err(DigestParseError::UnsupportedAlgorithm);
        }
        if hex.len() != 64 {
            return Err(DigestParseError::InvalidFormat);
        }
        if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(DigestParseError::InvalidHex);
        }
        Ok(Self {
            hex: hex.to_ascii_lowercase(),
        })
    }

    pub fn as_str(&self) -> String {
        format!("sha256:{}", self.hex)
    }

    pub fn hex(&self) -> &str {
        &self.hex
    }

    pub fn prefix2(&self) -> &str {
        // safe because hex length is 64
        &self.hex[..2]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_valid_sha256() {
        let d = Digest::parse(
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )
        .expect("valid digest");
        assert_eq!(
            d.hex(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
        assert_eq!(d.prefix2(), "01");
        assert_eq!(
            d.as_str(),
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn parse_rejects_non_sha256() {
        let err = Digest::parse("sha1:abcd").unwrap_err();
        matches!(
            err,
            DigestParseError::UnsupportedAlgorithm | DigestParseError::InvalidFormat
        );
    }

    #[test]
    fn parse_rejects_wrong_length() {
        let err = Digest::parse("sha256:abcd").unwrap_err();
        assert!(matches!(err, DigestParseError::InvalidFormat));
    }

    #[test]
    fn parse_rejects_non_hex() {
        let err = Digest::parse(
            "sha256:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
        )
        .unwrap_err();
        assert!(matches!(err, DigestParseError::InvalidHex));
    }
}
