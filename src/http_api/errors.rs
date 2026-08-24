use axum::{Json, http::StatusCode, response::IntoResponse};
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

pub fn not_implemented() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "UNSUPPORTED",
            message: "not implemented".to_string(),
            detail: None,
        }],
    };
    (StatusCode::NOT_IMPLEMENTED, Json(body))
}

pub fn insufficient_storage() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "INSUFFICIENT_STORAGE",
            message: "insufficient storage".to_string(),
            detail: None,
        }],
    };
    (StatusCode::INSUFFICIENT_STORAGE, Json(body))
}

pub fn payload_too_large() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "TOO_LARGE",
            message: "request body too large".to_string(),
            detail: None,
        }],
    };
    (StatusCode::PAYLOAD_TOO_LARGE, Json(body))
}

pub fn name_invalid() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "NAME_INVALID",
            message: "invalid repository name".to_string(),
            detail: None,
        }],
    };
    (StatusCode::BAD_REQUEST, Json(body))
}

pub fn name_unknown() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "NAME_UNKNOWN",
            message: "repository name unknown".to_string(),
            detail: None,
        }],
    };
    (StatusCode::NOT_FOUND, Json(body))
}

pub fn digest_invalid() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "DIGEST_INVALID",
            message: "invalid digest".to_string(),
            detail: None,
        }],
    };
    (StatusCode::BAD_REQUEST, Json(body))
}

pub fn tag_invalid() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "TAG_INVALID",
            message: "invalid tag".to_string(),
            detail: None,
        }],
    };
    (StatusCode::BAD_REQUEST, Json(body))
}

pub fn manifest_invalid() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "MANIFEST_INVALID",
            message: "invalid manifest".to_string(),
            detail: None,
        }],
    };
    (StatusCode::BAD_REQUEST, Json(body))
}

pub fn blob_unknown() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "BLOB_UNKNOWN",
            message: "blob unknown".to_string(),
            detail: None,
        }],
    };
    (StatusCode::NOT_FOUND, Json(body))
}

pub fn blob_in_use(message: &str) -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "BLOB_IN_USE",
            message: message.to_string(),
            detail: None,
        }],
    };
    (StatusCode::CONFLICT, Json(body))
}

pub fn manifest_unknown() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "MANIFEST_UNKNOWN",
            message: "manifest unknown".to_string(),
            detail: None,
        }],
    };
    (StatusCode::NOT_FOUND, Json(body))
}

pub fn blob_upload_unknown() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "BLOB_UPLOAD_UNKNOWN",
            message: "blob upload unknown".to_string(),
            detail: None,
        }],
    };
    (StatusCode::NOT_FOUND, Json(body))
}

pub fn blob_upload_invalid(message: &str) -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "BLOB_UPLOAD_INVALID",
            message: message.to_string(),
            detail: None,
        }],
    };
    (StatusCode::PAYLOAD_TOO_LARGE, Json(body))
}

pub fn denied(message: &str) -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "DENIED",
            message: message.to_string(),
            detail: None,
        }],
    };
    (StatusCode::FORBIDDEN, Json(body))
}

pub fn internal_error() -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "UNKNOWN",
            message: "internal error".to_string(),
            detail: None,
        }],
    };
    (StatusCode::INTERNAL_SERVER_ERROR, Json(body))
}

pub fn request_timeout(message: &str) -> impl IntoResponse {
    let body = ErrorBody {
        errors: vec![RegistryErrorItem {
            code: "UNKNOWN",
            message: message.to_string(),
            detail: None,
        }],
    };
    (StatusCode::REQUEST_TIMEOUT, Json(body))
}

#[allow(dead_code)]
pub fn warning_header_value(code: u16, agent: &str, text: &str) -> String {
    format!(r#"{} {} "{}""#, code, agent, text.replace('"', "\\\""))
}
