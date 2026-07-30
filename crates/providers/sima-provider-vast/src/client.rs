//! [`VastClient`]: the authenticated HTTP boundary every backend call goes
//! through.
//!
//! The client owns the API root, the key, and the connection pool, and it
//! turns each call into either a parsed answer or an [`Error::Provider`]
//! naming the operation. Callers read [`Answer::status`] when the status
//! itself carries meaning — an offer already taken, an instance already
//! gone — and otherwise hand the answer to [`Answer::ok`].

use serde_json::Value;
use sima_core::{Error, Result};
use ureq::Agent;
use ureq::http::Response;
use ureq::{Body, RequestBuilder};

/// One answer from the API: the status and the parsed JSON body.
pub(crate) struct Answer {
    /// The HTTP status the API answered with.
    pub(crate) status: u16,
    /// The response body, parsed as JSON.
    pub(crate) body: Value,
}

impl Answer {
    /// Whether the API answered with a success status.
    pub(crate) fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The body's `error` field, which the API sets when it names a
    /// failure.
    pub(crate) fn error(&self) -> Option<&str> {
        self.body.get("error")?.as_str()
    }

    /// The body's `msg` field, the human-readable detail the API sets
    /// beside `error` — the part that states what was actually wrong.
    pub(crate) fn msg(&self) -> Option<&str> {
        self.body.get("msg")?.as_str()
    }

    /// The body of a successful answer, or an [`Error::Provider`] naming
    /// `operation`, the status, and the error the body names.
    pub(crate) fn ok(self, operation: &str) -> Result<Value> {
        if self.is_success() {
            return Ok(self.body);
        }
        Err(Error::Provider(self.failure(operation)))
    }

    /// How this answer reads as a failure of `operation`.
    pub(crate) fn failure(&self, operation: &str) -> String {
        match (self.error(), self.msg()) {
            (Some(error), Some(msg)) => {
                format!("{operation}: HTTP {}: {error}: {msg}", self.status)
            }
            (Some(error), None) => format!("{operation}: HTTP {}: {error}", self.status),
            (None, Some(msg)) => format!("{operation}: HTTP {}: {msg}", self.status),
            (None, None) => format!("{operation}: HTTP {}", self.status),
        }
    }
}

/// An authenticated client for one API root.
pub(crate) struct VastClient {
    /// The API root every path is appended to.
    base_url: String,
    /// The key sent as a bearer token on every request.
    api_key: String,
    /// The connection pool the calls share.
    agent: Agent,
}

impl VastClient {
    /// A client reaching `base_url`, authenticating with `api_key`.
    pub(crate) fn new(base_url: &str, api_key: &str) -> VastClient {
        // Statuses carry meaning this backend maps itself, so the agent
        // hands every answer back rather than turning some into transport
        // errors.
        let agent: Agent = Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        VastClient {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            agent,
        }
    }

    /// `GET path`.
    pub(crate) fn get(&self, path: &str, operation: &str) -> Result<Answer> {
        let request = self.authed(self.agent.get(self.url(path)));
        answer(request.call(), operation)
    }

    /// `POST path` with `body` as JSON.
    pub(crate) fn post(&self, path: &str, body: &Value, operation: &str) -> Result<Answer> {
        let request = self.authed(self.agent.post(self.url(path)));
        answer(request.send_json(body), operation)
    }

    /// `PUT path` with `body` as JSON.
    pub(crate) fn put(&self, path: &str, body: &Value, operation: &str) -> Result<Answer> {
        let request = self.authed(self.agent.put(self.url(path)));
        answer(request.send_json(body), operation)
    }

    /// `DELETE path`.
    pub(crate) fn delete(&self, path: &str, operation: &str) -> Result<Answer> {
        let request = self.authed(self.agent.delete(self.url(path)));
        answer(request.call(), operation)
    }

    /// The absolute URL for `path`, which starts with `/`.
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// The request with the bearer token attached. The parameter is the
    /// builder's typestate, so a request with a body and one without are
    /// authenticated the same way.
    fn authed<B>(&self, request: RequestBuilder<B>) -> RequestBuilder<B> {
        request.header("Authorization", format!("Bearer {}", self.api_key))
    }
}

/// Turns a call's outcome into an [`Answer`], mapping a transport failure
/// and a body that is not JSON to [`Error::Provider`] naming `operation`.
///
/// The JSON mapping applies whatever the status is. The classifications
/// callers draw from a status — an instance gone, an offer taken, a destroy
/// that has already happened — are grounded in the API's own answers, which
/// are JSON; a body that is not JSON means an intermediary answered, and
/// reading its 404 as absence would report a machine still running and
/// billed as destroyed.
fn answer(
    outcome: std::result::Result<Response<Body>, ureq::Error>,
    operation: &str,
) -> Result<Answer> {
    let response = outcome.map_err(|failure| Error::Provider(format!("{operation}: {failure}")))?;
    let status = response.status().as_u16();
    let text = response
        .into_body()
        .read_to_string()
        .map_err(|failure| Error::Provider(format!("{operation}: read response: {failure}")))?;
    let body = serde_json::from_str(&text).map_err(|failure| {
        Error::Provider(format!(
            "{operation}: HTTP {status}: the response body is not JSON: {failure}"
        ))
    })?;
    Ok(Answer { status, body })
}

#[cfg(test)]
mod tests {
    use super::VastClient;
    use crate::test_server::{ScriptedAnswer, TestServer};
    use sima_core::{Error, Result};

    /// A scripted answer with `status` and `body`.
    fn answer(status: u16, body: &str) -> ScriptedAnswer {
        ScriptedAnswer {
            status,
            body: body.to_string(),
        }
    }

    #[test]
    fn every_request_carries_the_key_as_a_bearer_token() -> Result<()> {
        let server = TestServer::new(vec![
            answer(200, "{}"),
            answer(200, "{}"),
            answer(200, "{}"),
            answer(200, "{}"),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        client.get("/api/v0/instances/7/", "show instance")?;
        client.post("/api/v0/bundles/", &serde_json::json!({}), "list offers")?;
        client.put("/api/v0/asks/3/", &serde_json::json!({}), "create instance")?;
        client.delete("/api/v0/instances/7/", "destroy instance")?;
        let requests = server.requests();
        let methods: Vec<&str> = requests.iter().map(|r| r.method.as_str()).collect();
        assert_eq!(methods, vec!["GET", "POST", "PUT", "DELETE"]);
        for request in &requests {
            assert_eq!(request.authorization.as_deref(), Some("Bearer k-secret"));
        }
        Ok(())
    }

    #[test]
    fn a_request_reaches_the_path_under_the_configured_root() -> Result<()> {
        let server = TestServer::new(vec![answer(200, "{}")]);
        let client = VastClient::new(&format!("{}/", server.url()), "k-secret");
        client.get("/api/v0/instances/7/", "show instance")?;
        assert_eq!(server.requests()[0].path, "/api/v0/instances/7/");
        Ok(())
    }

    #[test]
    fn a_failure_the_body_names_reaches_the_caller_with_the_operation() {
        let server = TestServer::new(vec![answer(
            401,
            r#"{"success": false, "error": "unauthorized"}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-wrong");
        let outcome = client
            .get("/api/v0/instances/7/", "show instance")
            .and_then(|answer| answer.ok("show instance"));
        assert!(matches!(
            outcome,
            Err(Error::Provider(message))
                if message == "show instance: HTTP 401: unauthorized"
        ));
    }

    #[test]
    fn a_failure_carrying_detail_reaches_the_caller_with_it() {
        let server = TestServer::new(vec![answer(
            400,
            r#"{"success": false, "error": "invalid_args", "msg": "disk too small"}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        let outcome = client
            .get("/api/v0/instances/7/", "create instance")
            .and_then(|answer| answer.ok("create instance"));
        assert!(matches!(
            outcome,
            Err(Error::Provider(message))
                if message == "create instance: HTTP 400: invalid_args: disk too small"
        ));
    }

    #[test]
    fn a_failure_naming_nothing_reaches_the_caller_as_its_status() {
        let server = TestServer::new(vec![answer(500, "{}")]);
        let client = VastClient::new(&server.url(), "k-secret");
        let outcome = client
            .get("/api/v0/instances/7/", "show instance")
            .and_then(|answer| answer.ok("show instance"));
        assert!(matches!(
            outcome,
            Err(Error::Provider(message)) if message == "show instance: HTTP 500"
        ));
    }

    #[test]
    fn a_body_that_is_not_json_is_a_provider_error_naming_the_operation() {
        let server = TestServer::new(vec![answer(200, "<html>maintenance</html>")]);
        let client = VastClient::new(&server.url(), "k-secret");
        let outcome = client.get("/api/v0/instances/7/", "show instance");
        assert!(matches!(
            outcome,
            Err(Error::Provider(message)) if message.starts_with("show instance: HTTP 200: the response body is not JSON")
        ));
    }

    #[test]
    fn a_root_nothing_listens_on_is_a_provider_error_naming_the_operation() {
        // Port 1 on loopback carries no listener, so the connection fails
        // before any answer exists.
        let client = VastClient::new("http://127.0.0.1:1", "k-secret");
        let outcome = client.get("/api/v0/instances/7/", "show instance");
        assert!(matches!(
            outcome,
            Err(Error::Provider(message)) if message.starts_with("show instance: ")
        ));
    }

    #[test]
    fn a_successful_answer_yields_its_parsed_body() -> Result<()> {
        let server = TestServer::new(vec![answer(200, r#"{"success": true, "id": 42}"#)]);
        let client = VastClient::new(&server.url(), "k-secret");
        let body = client
            .get("/api/v0/instances/42/", "show instance")?
            .ok("show instance")?;
        assert_eq!(body["id"], 42);
        Ok(())
    }

    #[test]
    fn a_body_sent_reaches_the_server_verbatim() -> Result<()> {
        let server = TestServer::new(vec![answer(200, "{}")]);
        let client = VastClient::new(&server.url(), "k-secret");
        client.put(
            "/api/v0/asks/3/",
            &serde_json::json!({"image": "ghcr.io/owner/sima-worker", "disk": 64}),
            "create instance",
        )?;
        let sent = server.requests()[0].json();
        assert_eq!(sent["image"], "ghcr.io/owner/sima-worker");
        assert_eq!(sent["disk"], 64);
        Ok(())
    }
}
