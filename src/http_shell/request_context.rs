use uuid::Uuid;

use super::security::AuthenticatedActor;

/// Request-scoped identity propagated from authentication to every handler.
#[derive(Clone, Debug)]
pub(crate) struct RequestContext {
    request_id: String,
    actor: AuthenticatedActor,
}

impl RequestContext {
    pub(crate) fn new_request_id() -> String {
        Uuid::new_v4().simple().to_string()
    }

    pub(crate) fn with_request_id(request_id: String, actor: AuthenticatedActor) -> Self {
        Self { request_id, actor }
    }

    pub(crate) fn request_id(&self) -> &str {
        &self.request_id
    }

    pub(crate) fn actor(&self) -> &AuthenticatedActor {
        &self.actor
    }
}

#[cfg(test)]
mod tests {
    use super::RequestContext;
    use crate::http_shell::security::AuthenticatedActor;

    #[test]
    fn request_ids_have_a_constant_safe_shape() {
        let first = RequestContext::with_request_id(
            RequestContext::new_request_id(),
            AuthenticatedActor::AuthenticationDisabled,
        );
        let second = RequestContext::with_request_id(
            RequestContext::new_request_id(),
            AuthenticatedActor::AuthenticationDisabled,
        );
        assert_ne!(first.request_id(), second.request_id());
        assert_eq!(first.request_id().len(), 32);
        assert!(
            first
                .request_id()
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        );
    }
}
