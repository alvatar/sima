//! The instances the account holds: what the API reports about one, and
//! the call that reads it.

use serde::Deserialize;
use sima_core::Result;

use crate::client::VastClient;

/// The status a missing instance answers with. Only this status reads as
/// absence: any other failure is a fault, and reading it as absence would
/// report a machine still running and billed as gone.
const NOT_FOUND: u16 = 404;

/// One instance as the API reports it. The type never leaves this crate.
#[derive(Deserialize)]
pub(crate) struct InstanceRow {
    /// The hourly rate the account is charged for the instance.
    pub(crate) dph_total: f64,
}

/// The API's envelope around a single instance.
#[derive(Deserialize)]
struct InstanceEnvelope {
    /// The instance itself.
    instances: InstanceRow,
}

/// The instance `id`, or `None` when the account holds no such instance.
/// A failure of any other kind reaches the caller as `operation` failing.
pub(crate) fn show(client: &VastClient, id: &str, operation: &str) -> Result<Option<InstanceRow>> {
    let answer = client.get(&show_path(id), operation)?;
    if answer.status == NOT_FOUND {
        return Ok(None);
    }
    let body = answer.ok(operation)?;
    let envelope: InstanceEnvelope = serde_json::from_value(body)
        .map_err(|failure| sima_core::Error::Provider(format!("{operation}: {failure}")))?;
    Ok(Some(envelope.instances))
}

/// The path reading one instance.
fn show_path(id: &str) -> String {
    format!("/api/v0/instances/{id}/")
}
