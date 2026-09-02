//! The instances the account holds: what the API reports about one, the
//! walk over all of them, and the destroy that ends a rental.

use serde::Deserialize;
use sima_core::{Error, Result};
use sima_provider::{InstanceId, InstanceStatus, SshEndpoint, TaggedInstance};

use crate::client::VastClient;
use crate::price;

/// The status a missing instance answers with. Only this status reads as
/// absence: any other failure is a fault, and reading it as absence would
/// report a machine still running and billed as gone.
const NOT_FOUND: u16 = 404;

/// The status of an instance that is up.
const RUNNING: &str = "running";

/// The user a rental is reached as over SSH.
const SSH_USER: &str = "root";

/// The listing endpoint.
const LIST_PATH: &str = "/api/v1/instances/";

/// Instances one listing page carries, which is the largest page the API
/// serves.
const PAGE_SIZE: u32 = 25;

/// What the listing walk is called in failures.
const LIST_OPERATION: &str = "list instances";

/// What reading one instance is called in failures.
const SHOW_OPERATION: &str = "show instance";

/// What ending a rental is called in failures.
const DESTROY_OPERATION: &str = "destroy instance";

/// One instance as the API reports it. The type never leaves this crate.
#[derive(Deserialize)]
pub(crate) struct InstanceRow {
    /// The API's identifier for the instance.
    id: i64,
    /// The label the instance was created under, which the API reports as
    /// null for an instance created without one.
    label: Option<String>,
    /// Where the instance is in its lifecycle.
    actual_status: Option<String>,
    /// The host the instance is reached at once it is up.
    ssh_host: Option<String>,
    /// The port the instance is reached at once it is up.
    ssh_port: Option<u16>,
    /// The hourly rate the account is charged for the instance, which the
    /// API reports for a billed instance and an anomalous row may omit.
    /// Only the rental path consumes it, and only there is its presence
    /// demanded: readiness polling and the reconciliation listing read the
    /// other fields, so a row missing a rate still normalizes and still
    /// lists.
    pub(crate) dph_total: Option<f64>,
}

/// The API's envelope around a single instance. The row is null in the
/// window right after a rental is created, before the API materializes the
/// read — a pending row, distinct from the 404 of an instance the account
/// does not hold.
#[derive(Deserialize)]
struct InstanceEnvelope {
    /// The instance itself, or null while the row is still materializing.
    instances: Option<InstanceRow>,
}

/// What the API answers when one instance is read.
pub(crate) enum ShowAnswer {
    /// The instance's row.
    Row(InstanceRow),
    /// The rental exists but its row has not materialized yet.
    Pending,
    /// The account holds no such instance.
    Absent,
}

/// One page of the listing.
#[derive(Deserialize)]
struct InstancePage {
    /// The instances on this page.
    instances: Vec<InstanceRow>,
    /// The cursor the next page is read with, which the API reports as
    /// null once the listing is exhausted.
    next_token: Option<String>,
}

/// The API's answer for the instance `id`. A failure of any other kind
/// reaches the caller as `operation` failing.
pub(crate) fn show(client: &VastClient, id: &str, operation: &str) -> Result<ShowAnswer> {
    let answer = client.get(&show_path(id), operation)?;
    if answer.status == NOT_FOUND {
        return Ok(ShowAnswer::Absent);
    }
    let body = answer.ok(operation)?;
    let envelope: InstanceEnvelope = serde_json::from_value(body)
        .map_err(|failure| Error::Provider(format!("{operation}: {failure}")))?;
    Ok(match envelope.instances {
        Some(row) => ShowAnswer::Row(row),
        None => ShowAnswer::Pending,
    })
}

/// The state the API reports for `id`. A pending row is an instance still
/// coming up: only the 404 of an instance the account does not hold reads
/// as gone, and readiness polling bounds how long pending may last.
pub(crate) fn status(client: &VastClient, id: &InstanceId) -> Result<InstanceStatus> {
    match show(client, &id.0, SHOW_OPERATION)? {
        ShowAnswer::Row(row) => Ok(row.state()),
        ShowAnswer::Pending => Ok(InstanceStatus::Provisioning),
        ShowAnswer::Absent => Ok(InstanceStatus::Gone),
    }
}

/// The most pages one listing walks.
///
/// The cursor comes from the service, so the walk is only as bounded as the
/// service is well-behaved. An account holding more instances than this has
/// problems a listing cannot fix, and the bound turns a cursor that never ends
/// into an error naming it instead of a loop that never returns.
const MAX_PAGES: usize = 1_000;

/// Every instance the account holds, with the tag it was created under.
///
/// An instance carrying no label is omitted: the listing's key is the tag,
/// and a row stating none carries nothing to match a ledger record by. The
/// omission costs no machine, because reconciliation serves this listing to
/// intent records alone — a live record names its instance id, and one the
/// listing omits is probed by id directly.
pub(crate) fn held(client: &VastClient) -> Result<Vec<TaggedInstance>> {
    let mut held = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_PAGES {
        let body = client
            .get(&list_path(cursor.as_deref()), LIST_OPERATION)?
            .ok(LIST_OPERATION)?;
        let page: InstancePage = serde_json::from_value(body)
            .map_err(|failure| Error::Provider(format!("{LIST_OPERATION}: {failure}")))?;
        held.extend(page.instances.into_iter().filter_map(|row| {
            let id = InstanceId(row.id.to_string());
            let price = row.dph_total.and_then(price::per_hour);
            row.label.map(|tag| TaggedInstance { id, tag, price })
        }));
        // The API reports a cursor for as long as a page follows, so an
        // absent one is the end of the listing.
        let Some(token) = page.next_token else {
            return Ok(held);
        };
        // A cursor equal to the one that fetched this page would ask for it
        // again forever. The service is not this side's to trust, so a repeat
        // is a provider fault rather than a loop.
        if cursor.as_deref() == Some(token.as_str()) {
            return Err(Error::Provider(format!(
                "{LIST_OPERATION}: the listing repeated its cursor {token:?}, so the pages do \
                 not advance"
            )));
        }
        cursor = Some(token);
    }
    Err(Error::Provider(format!(
        "{LIST_OPERATION}: the listing ran past {MAX_PAGES} pages without ending"
    )))
}

/// Ends the rental of `id`. An instance the account no longer holds is
/// success: guards and reconciliation may race each other and provider-side
/// expiry.
pub(crate) fn destroy(client: &VastClient, id: &InstanceId) -> Result<()> {
    let answer = client.delete(&show_path(&id.0), DESTROY_OPERATION)?;
    if answer.status == NOT_FOUND {
        return Ok(());
    }
    answer.ok(DESTROY_OPERATION)?;
    Ok(())
}

impl InstanceRow {
    /// The normalized state of this instance: an instance that is up and
    /// reports where to reach it is ready, and anything else is still
    /// coming up.
    fn state(&self) -> InstanceStatus {
        let up = self.actual_status.as_deref() == Some(RUNNING);
        match (up, self.ssh_host.as_deref(), self.ssh_port) {
            (true, Some(host), Some(port)) => InstanceStatus::Ready(SshEndpoint {
                host: host.to_string(),
                port,
                user: SSH_USER.to_string(),
            }),
            _ => InstanceStatus::Provisioning,
        }
    }
}

/// The path reading, or destroying, one instance.
fn show_path(id: &str) -> String {
    format!("/api/v0/instances/{id}/")
}

/// The path reading one listing page, continuing from `cursor` when one
/// page has already been read.
fn list_path(cursor: Option<&str>) -> String {
    match cursor {
        Some(token) => format!("{LIST_PATH}?limit={PAGE_SIZE}&after_token={token}"),
        None => format!("{LIST_PATH}?limit={PAGE_SIZE}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{destroy, held, status};
    use crate::client::VastClient;
    use crate::test_server::{ScriptedAnswer, TestServer};
    use sima_core::{Error, Result};
    use sima_provider::{InstanceId, InstanceStatus, Price, SshEndpoint};

    /// A scripted answer with `status` and `body`.
    fn answer(status: u16, body: &str) -> ScriptedAnswer {
        ScriptedAnswer {
            status,
            body: body.to_string(),
        }
    }

    /// The instance every status test asks about.
    fn instance() -> InstanceId {
        InstanceId("555".to_string())
    }

    #[test]
    fn an_instance_that_is_up_is_ready_at_the_endpoint_it_reports() -> Result<()> {
        let server = TestServer::new(vec![answer(
            200,
            r#"{"instances": {"id": 555, "actual_status": "running",
                "ssh_host": "ssh4.vast.ai", "ssh_port": 41231,
                "label": "sima-tag-0", "dph_total": 0.412}}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert_eq!(
            status(&client, &instance())?,
            InstanceStatus::Ready(SshEndpoint {
                host: "ssh4.vast.ai".to_string(),
                port: 41231,
                user: "root".to_string(),
            })
        );
        assert_eq!(server.requests()[0].path, "/api/v0/instances/555/");
        Ok(())
    }

    #[test]
    fn an_instance_still_loading_is_provisioning() -> Result<()> {
        let server = TestServer::new(vec![answer(
            200,
            r#"{"instances": {"id": 555, "actual_status": "loading",
                "label": "sima-tag-0", "dph_total": 0.412}}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert_eq!(status(&client, &instance())?, InstanceStatus::Provisioning);
        Ok(())
    }

    #[test]
    fn an_instance_up_without_an_endpoint_is_provisioning() -> Result<()> {
        // The machine reports itself running before the API publishes
        // where to reach it, and a rental with nowhere to connect is not
        // ready.
        let server = TestServer::new(vec![answer(
            200,
            r#"{"instances": {"id": 555, "actual_status": "running",
                "ssh_host": null, "ssh_port": null, "dph_total": 0.412}}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert_eq!(status(&client, &instance())?, InstanceStatus::Provisioning);
        Ok(())
    }

    #[test]
    fn a_pending_row_held_by_the_account_is_provisioning() -> Result<()> {
        let server = TestServer::new(vec![
            answer(200, r#"{"instances": null}"#),
            answer(
                200,
                r#"{"instances": [{"id": 555, "label": "sima-tag-0",
                    "dph_total": 0.412}], "next_token": null}"#,
            ),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert_eq!(status(&client, &instance())?, InstanceStatus::Provisioning);
        assert_eq!(server.requests().len(), 2);
        Ok(())
    }

    #[test]
    fn a_pending_row_absent_from_the_account_listing_is_gone() -> Result<()> {
        let server = TestServer::new(vec![
            answer(200, r#"{"instances": null}"#),
            answer(
                200,
                r#"{"instances": [{"id": 777, "label": "sima-tag-1",
                    "dph_total": 0.2}], "next_token": null}"#,
            ),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert_eq!(status(&client, &instance())?, InstanceStatus::Gone);
        Ok(())
    }

    #[test]
    fn a_failing_listing_for_a_pending_row_reaches_the_caller() {
        let server = TestServer::new(vec![
            answer(200, r#"{"instances": null}"#),
            answer(500, r#"{"success": false, "error": "listing_failed"}"#),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            status(&client, &instance()),
            Err(Error::Provider(message))
                if message == "list instances: HTTP 500: listing_failed"
        ));
    }

    #[test]
    fn an_instance_the_account_does_not_hold_is_gone() -> Result<()> {
        let server = TestServer::new(vec![answer(
            404,
            r#"{"success": false, "error": "no_such_instance"}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert_eq!(status(&client, &instance())?, InstanceStatus::Gone);
        Ok(())
    }

    #[test]
    fn a_failing_status_call_reaches_the_caller_rather_than_reading_as_gone() {
        let server = TestServer::new(vec![answer(
            401,
            r#"{"success": false, "error": "invalid_api_key"}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-wrong");
        assert!(matches!(
            status(&client, &instance()),
            Err(Error::Provider(message))
                if message == "show instance: HTTP 401: invalid_api_key"
        ));
    }

    #[test]
    fn an_absence_only_an_intermediary_states_is_a_fault_rather_than_gone() {
        // The API answers JSON, so an HTML 404 came from something between
        // this client and the API. Reading it as absence would report a
        // machine still running and billed as gone.
        let server = TestServer::new(vec![answer(404, "<html>not found</html>")]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            status(&client, &instance()),
            Err(Error::Provider(message))
                if message.starts_with("show instance: HTTP 404: the response body is not JSON")
        ));
    }

    #[test]
    fn a_listing_that_repeats_its_cursor_is_refused() -> Result<()> {
        // The cursor is the service's, so the walk is only as bounded as the
        // service is well-behaved: one that hands back the token that fetched
        // the page would be asked for it forever. The refusal names the token.
        let page = |token: &str| {
            answer(
                200,
                &format!(
                    r#"{{"instances": [{{"id": 1, "label": "sima-tag-1", "dph_total": 0.1}}],
                        "next_token": "{token}"}}"#
                ),
            )
        };
        let server = TestServer::new(vec![page("c-25"), page("c-25")]);
        let client = VastClient::new(&server.url(), "k-secret");
        let Err(Error::Provider(message)) = held(&client) else {
            panic!("expected a repeated cursor to be refused");
        };
        assert!(message.contains("c-25"), "names the cursor: {message}");
        // Two requests: the walk stopped at the repeat rather than going on.
        assert_eq!(server.requests().len(), 2);
        Ok(())
    }

    #[test]
    fn the_listing_walks_every_page_the_api_serves() -> Result<()> {
        let server = TestServer::new(vec![
            answer(
                200,
                r#"{"instances": [{"id": 1, "label": "sima-tag-1", "dph_total": 0.1}],
                    "next_token": "c-25"}"#,
            ),
            answer(
                200,
                r#"{"instances": [{"id": 2, "label": "sima-tag-2", "dph_total": 0.2}],
                    "next_token": null}"#,
            ),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        let instances = held(&client)?;
        let tags: Vec<&str> = instances.iter().map(|held| held.tag.as_str()).collect();
        assert_eq!(tags, vec!["sima-tag-1", "sima-tag-2"]);
        assert_eq!(instances[0].id, InstanceId("1".to_string()));
        // The rate each row states is what the account is billed, which is
        // what a rental closed out from the listing is charged.
        assert_eq!(instances[0].price, Some(Price(100_000)));
        assert_eq!(instances[1].price, Some(Price(200_000)));
        let requests = server.requests();
        assert_eq!(requests[0].path, "/api/v1/instances/?limit=25");
        assert_eq!(
            requests[1].path,
            "/api/v1/instances/?limit=25&after_token=c-25"
        );
        Ok(())
    }

    #[test]
    fn an_instance_carrying_no_label_is_omitted_from_the_listing() -> Result<()> {
        let server = TestServer::new(vec![answer(
            200,
            r#"{"instances": [{"id": 1, "label": null, "dph_total": 0.1},
                              {"id": 2, "label": "sima-tag-2", "dph_total": 0.2}],
                "next_token": null}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        let instances = held(&client)?;
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].id, InstanceId("2".to_string()));
        assert_eq!(instances[0].tag, "sima-tag-2");
        Ok(())
    }

    #[test]
    fn an_instance_reporting_no_rate_still_lists() -> Result<()> {
        // Reconciliation exists to reap strays, and a stray is the row most
        // likely to be anomalous, so a missing rate never costs the listing.
        let server = TestServer::new(vec![answer(
            200,
            r#"{"instances": [{"id": 1, "label": "sima-tag-1"},
                              {"id": 2, "label": "sima-tag-2", "dph_total": null}],
                "next_token": null}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        let instances = held(&client)?;
        let tags: Vec<&str> = instances.iter().map(|held| held.tag.as_str()).collect();
        assert_eq!(tags, vec!["sima-tag-1", "sima-tag-2"]);
        // A row stating no rate lists without one, and a close-out reading
        // this listing falls back to the record's rate.
        assert_eq!(instances[0].price, None);
        assert_eq!(instances[1].price, None);
        Ok(())
    }

    #[test]
    fn an_instance_reporting_an_anomalous_rate_lists_with_no_rate() -> Result<()> {
        let server = TestServer::new(vec![answer(
            200,
            r#"{"instances": [{"id": 1, "label": "sima-tag-1", "dph_total": -0.1}],
                "next_token": null}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        let instances = held(&client)?;
        // The machine still lists, so reconciliation can still reap it; the
        // rate no price can be read from leaves the record's own standing.
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].price, None);
        Ok(())
    }

    #[test]
    fn an_instance_reporting_no_rate_still_normalizes() -> Result<()> {
        let server = TestServer::new(vec![answer(
            200,
            r#"{"instances": {"id": 555, "actual_status": "running",
                "ssh_host": "ssh4.vast.ai", "ssh_port": 41231,
                "label": "sima-tag-0"}}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert_eq!(
            status(&client, &instance())?,
            InstanceStatus::Ready(SshEndpoint {
                host: "ssh4.vast.ai".to_string(),
                port: 41231,
                user: "root".to_string(),
            })
        );
        Ok(())
    }

    #[test]
    fn an_account_holding_nothing_lists_nothing() -> Result<()> {
        let server = TestServer::new(vec![answer(
            200,
            r#"{"instances": [], "next_token": null}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(held(&client)?.is_empty());
        Ok(())
    }

    #[test]
    fn destroying_an_instance_ends_its_rental() -> Result<()> {
        let server = TestServer::new(vec![answer(200, r#"{"success": true}"#)]);
        let client = VastClient::new(&server.url(), "k-secret");
        destroy(&client, &instance())?;
        let request = &server.requests()[0];
        assert_eq!(request.method, "DELETE");
        assert_eq!(request.path, "/api/v0/instances/555/");
        Ok(())
    }

    #[test]
    fn destroying_an_instance_already_gone_is_success() -> Result<()> {
        let server = TestServer::new(vec![answer(
            404,
            r#"{"success": false, "error": "not_found"}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        destroy(&client, &instance())
    }

    #[test]
    fn a_destroy_the_api_rejects_reaches_the_caller() {
        let server = TestServer::new(vec![answer(
            500,
            r#"{"success": false, "error": "internal"}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            destroy(&client, &instance()),
            Err(Error::Provider(message))
                if message == "destroy instance: HTTP 500: internal"
        ));
    }
}
