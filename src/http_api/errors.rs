use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub errors: Vec<RegistryErrorItem>,
}

#[derive(Debug, Serialize)]
pub struct RegistryErrorItem {
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

fn error_response(status: StatusCode, body: ErrorBody) -> Response {
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        header::HeaderName::from_static("docker-distribution-api-version"),
        HeaderValue::from_static("registry/2.0"),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&bytes.len().to_string()).unwrap(),
    );
    (status, headers, Body::from(bytes)).into_response()
}

pub fn not_implemented() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "UNSUPPORTED",
            message: "not implemented".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::NOT_IMPLEMENTED, body)
}

pub fn method_not_allowed(allow: &'static str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "UNSUPPORTED",
            message: "method not allowed".to_string(),
            detail: None,
        }],
    };
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        header::HeaderName::from_static("docker-distribution-api-version"),
        HeaderValue::from_static("registry/2.0"),
    );
    headers.insert(header::ALLOW, HeaderValue::from_static(allow));
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&bytes.len().to_string()).unwrap(),
    );
    (StatusCode::METHOD_NOT_ALLOWED, headers, Body::from(bytes)).into_response()
}

pub fn insufficient_storage() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "INSUFFICIENT_STORAGE",
            message: "insufficient storage".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::INSUFFICIENT_STORAGE, body)
}

pub fn payload_too_large() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "TOO_LARGE",
            message: "request body too large".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::PAYLOAD_TOO_LARGE, body)
}

pub fn unauthorized(message: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "UNAUTHORIZED",
            message: message.to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::UNAUTHORIZED, body)
}

pub fn name_invalid() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "NAME_INVALID",
            message: "invalid repository name".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::BAD_REQUEST, body)
}

pub fn name_unknown() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "NAME_UNKNOWN",
            message: "repository name unknown".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::NOT_FOUND, body)
}

pub fn digest_invalid() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "DIGEST_INVALID",
            message: "invalid digest".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::BAD_REQUEST, body)
}

pub fn tag_invalid() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "TAG_INVALID",
            message: "invalid tag".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::BAD_REQUEST, body)
}

pub fn tag_unknown() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "TAG_UNKNOWN",
            message: "tag unknown".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::NOT_FOUND, body)
}

pub fn manifest_invalid() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "MANIFEST_INVALID",
            message: "invalid manifest".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::BAD_REQUEST, body)
}

pub fn manifest_blob_unknown(digest: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "MANIFEST_BLOB_UNKNOWN",
            message: "blob unknown to registry".to_string(),
            detail: Some(serde_json::json!({ "digest": digest })),
        }],
    };
    error_response(StatusCode::BAD_REQUEST, body)
}

#[allow(dead_code)]
pub fn manifest_unverified(message: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "MANIFEST_UNVERIFIED",
            message: message.to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::BAD_REQUEST, body)
}

pub fn blob_unknown() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "BLOB_UNKNOWN",
            message: "blob unknown".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::NOT_FOUND, body)
}

pub fn blob_in_use(message: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "BLOB_IN_USE",
            message: message.to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::CONFLICT, body)
}

pub fn manifest_unknown() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "MANIFEST_UNKNOWN",
            message: "manifest unknown".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::NOT_FOUND, body)
}

pub fn blob_upload_unknown() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "BLOB_UPLOAD_UNKNOWN",
            message: "blob upload unknown".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::NOT_FOUND, body)
}

pub fn blob_upload_invalid(message: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "BLOB_UPLOAD_INVALID",
            message: message.to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::BAD_REQUEST, body)
}

pub fn size_invalid(message: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "SIZE_INVALID",
            message: message.to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::BAD_REQUEST, body)
}

/// Standard OCI error response for invalid upload chunk range:
/// HTTP 416 Range Not Satisfiable with standard OCI code `BLOB_UPLOAD_INVALID`.
pub fn range_invalid(message: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "BLOB_UPLOAD_INVALID",
            message: message.to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::RANGE_NOT_SATISFIABLE, body)
}

/// Compatibility extension error response for nonstandard clients expecting `RANGE_INVALID`.
#[allow(dead_code)]
pub fn range_invalid_compat(message: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "RANGE_INVALID",
            message: message.to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::RANGE_NOT_SATISFIABLE, body)
}

pub fn denied(message: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "DENIED",
            message: message.to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::FORBIDDEN, body)
}

pub fn internal_error() -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "UNKNOWN",
            message: "internal error".to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::INTERNAL_SERVER_ERROR, body)
}

pub fn request_timeout(message: &str) -> Response {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "UNKNOWN",
            message: message.to_string(),
            detail: None,
        }],
    };
    error_response(StatusCode::REQUEST_TIMEOUT, body)
}

#[allow(dead_code)]
pub fn warning_header_value(code: u16, agent: &str, text: &str) -> String {
    format!(r#"{} {} "{}""#, code, agent, text.replace('"', "\\\""))
}
