use crate::security::RepoAction;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OciRoute {
    V2Ping,
    Catalog,
    ExtensionDiscovery { repo: Option<String> },
    UploadInitiate { repo: String },
    UploadSession { repo: String, uuid: String },
    Blob { repo: String, digest: String },
    Manifest { repo: String, reference: String },
    TagsList { repo: String },
    TagDelete { repo: String, tag: String },
    Referrers { repo: String, digest: String },
    Unknown { path: String },
}

impl OciRoute {
    pub fn parse(path: &str) -> Self {
        let trimmed = path.trim();
        if trimmed == "/v2" || trimmed == "/v2/" {
            return OciRoute::V2Ping;
        }

        let Some(rest) = trimmed.strip_prefix("/v2/") else {
            return OciRoute::Unknown {
                path: path.to_string(),
            };
        };

        let segments: Vec<&str> = rest.split('/').filter(|s| !s.is_empty()).collect();
        if segments.is_empty() {
            return OciRoute::V2Ping;
        }

        // /v2/_catalog
        if segments.len() == 1 && segments[0] == "_catalog" {
            return OciRoute::Catalog;
        }

        // Global /v2/_oci/ext/discover
        if segments.len() == 3
            && segments[0] == "_oci"
            && segments[1] == "ext"
            && segments[2] == "discover"
        {
            return OciRoute::ExtensionDiscovery { repo: None };
        }

        // Repo-level /v2/<name>/_oci/ext/discover
        if segments.len() >= 4
            && segments[segments.len() - 3] == "_oci"
            && segments[segments.len() - 2] == "ext"
            && segments[segments.len() - 1] == "discover"
        {
            let repo = segments[..segments.len() - 3].join("/");
            return OciRoute::ExtensionDiscovery { repo: Some(repo) };
        }

        // Tags endpoints:
        // /v2/<name>/tags/list
        if segments.len() >= 2
            && segments[segments.len() - 2] == "tags"
            && segments[segments.len() - 1] == "list"
        {
            let repo = segments[..segments.len() - 2].join("/");
            return OciRoute::TagsList { repo };
        }

        // /v2/<name>/tags/reference/<tag>
        if segments.len() >= 3
            && segments[segments.len() - 3] == "tags"
            && segments[segments.len() - 2] == "reference"
        {
            let repo = segments[..segments.len() - 3].join("/");
            let tag = segments[segments.len() - 1].to_string();
            return OciRoute::TagDelete { repo, tag };
        }

        // Referrers endpoint:
        // /v2/<name>/referrers/<digest>
        if segments.len() >= 2 && segments[segments.len() - 2] == "referrers" {
            let repo = segments[..segments.len() - 2].join("/");
            let digest = segments[segments.len() - 1].to_string();
            return OciRoute::Referrers { repo, digest };
        }

        // Manifests endpoint:
        // /v2/<name>/manifests/<reference>
        if segments.len() >= 2 && segments[segments.len() - 2] == "manifests" {
            let repo = segments[..segments.len() - 2].join("/");
            let reference = segments[segments.len() - 1].to_string();
            return OciRoute::Manifest { repo, reference };
        }

        // Upload initiation:
        // /v2/<name>/blobs/uploads/
        if segments.len() >= 2
            && segments[segments.len() - 2] == "blobs"
            && segments[segments.len() - 1] == "uploads"
        {
            let repo = segments[..segments.len() - 2].join("/");
            return OciRoute::UploadInitiate { repo };
        }

        // Upload session:
        // /v2/<name>/blobs/uploads/<uuid>
        if segments.len() >= 3
            && segments[segments.len() - 3] == "blobs"
            && segments[segments.len() - 2] == "uploads"
        {
            let repo = segments[..segments.len() - 3].join("/");
            let uuid = segments[segments.len() - 1].to_string();
            return OciRoute::UploadSession { repo, uuid };
        }

        // Blob endpoint:
        // /v2/<name>/blobs/<digest>
        if segments.len() >= 2 && segments[segments.len() - 2] == "blobs" {
            let repo = segments[..segments.len() - 2].join("/");
            let digest = segments[segments.len() - 1].to_string();
            return OciRoute::Blob { repo, digest };
        }

        OciRoute::Unknown {
            path: path.to_string(),
        }
    }

    pub fn repository(&self) -> Option<&str> {
        match self {
            OciRoute::V2Ping | OciRoute::Catalog | OciRoute::Unknown { .. } => None,
            OciRoute::ExtensionDiscovery { repo } => repo.as_deref(),
            OciRoute::UploadInitiate { repo }
            | OciRoute::UploadSession { repo, .. }
            | OciRoute::Blob { repo, .. }
            | OciRoute::Manifest { repo, .. }
            | OciRoute::TagsList { repo }
            | OciRoute::TagDelete { repo, .. }
            | OciRoute::Referrers { repo, .. } => Some(repo),
        }
    }

    pub fn required_action(&self, method: &http::Method) -> Option<RepoAction> {
        match self {
            OciRoute::V2Ping | OciRoute::ExtensionDiscovery { .. } | OciRoute::Catalog => None,
            OciRoute::UploadInitiate { .. } => Some(RepoAction::Push),
            OciRoute::UploadSession { .. } => match *method {
                http::Method::GET | http::Method::HEAD => Some(RepoAction::Pull),
                http::Method::PATCH | http::Method::PUT | http::Method::POST | http::Method::DELETE => {
                    Some(RepoAction::Push)
                }
                _ => Some(RepoAction::Push),
            },
            OciRoute::Blob { .. } => match *method {
                http::Method::GET | http::Method::HEAD => Some(RepoAction::Pull),
                http::Method::DELETE => Some(RepoAction::Delete),
                _ => Some(RepoAction::Push),
            },
            OciRoute::Manifest { .. } => match *method {
                http::Method::GET | http::Method::HEAD => Some(RepoAction::Pull),
                http::Method::PUT => Some(RepoAction::Push),
                http::Method::DELETE => Some(RepoAction::Delete),
                _ => Some(RepoAction::Pull),
            },
            OciRoute::TagsList { .. } | OciRoute::Referrers { .. } => match *method {
                http::Method::GET | http::Method::HEAD => Some(RepoAction::Pull),
                _ => Some(RepoAction::Pull),
            },
            OciRoute::TagDelete { .. } => Some(RepoAction::Delete),
            OciRoute::Unknown { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_parsing() {
        assert_eq!(OciRoute::parse("/v2"), OciRoute::V2Ping);
        assert_eq!(OciRoute::parse("/v2/"), OciRoute::V2Ping);
        assert_eq!(OciRoute::parse("/v2/_catalog"), OciRoute::Catalog);
        assert_eq!(
            OciRoute::parse("/v2/_oci/ext/discover"),
            OciRoute::ExtensionDiscovery { repo: None }
        );
        assert_eq!(
            OciRoute::parse("/v2/org/app/_oci/ext/discover"),
            OciRoute::ExtensionDiscovery {
                repo: Some("org/app".to_string())
            }
        );
        assert_eq!(
            OciRoute::parse("/v2/org/app/blobs/uploads/"),
            OciRoute::UploadInitiate {
                repo: "org/app".to_string()
            }
        );
        assert_eq!(
            OciRoute::parse("/v2/v2-repo/blobs/uploads/123-uuid"),
            OciRoute::UploadSession {
                repo: "v2-repo".to_string(),
                uuid: "123-uuid".to_string()
            }
        );
        assert_eq!(
            OciRoute::parse("/v2/2026/app/blobs/sha256:123"),
            OciRoute::Blob {
                repo: "2026/app".to_string(),
                digest: "sha256:123".to_string()
            }
        );
        assert_eq!(
            OciRoute::parse("/v2/org/app/manifests/v1.0.0"),
            OciRoute::Manifest {
                repo: "org/app".to_string(),
                reference: "v1.0.0".to_string()
            }
        );
        assert_eq!(
            OciRoute::parse("/v2/org/app/tags/list"),
            OciRoute::TagsList {
                repo: "org/app".to_string()
            }
        );
        assert_eq!(
            OciRoute::parse("/v2/org/app/tags/reference/tag1"),
            OciRoute::TagDelete {
                repo: "org/app".to_string(),
                tag: "tag1".to_string()
            }
        );
        assert_eq!(
            OciRoute::parse("/v2/org/app/referrers/sha256:123"),
            OciRoute::Referrers {
                repo: "org/app".to_string(),
                digest: "sha256:123".to_string()
            }
        );
    }
}
