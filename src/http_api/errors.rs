use axum::{http::StatusCode, response::IntoResponse, Json};
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
