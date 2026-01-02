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
        let (algo, hex) = input.split_once(':').ok_or(DigestParseError::InvalidFormat)?;
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
