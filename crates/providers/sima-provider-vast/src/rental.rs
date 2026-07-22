//! Renting one offer: the create call, the rate the created instance is
//! charged at, and the offer another renter took first.

use serde_json::{Value, json};
use sima_core::{Error, Result};
use sima_provider::{Instance, InstanceId, OfferId, Provision};

use crate::client::VastClient;
use crate::config::VastConfig;
use crate::instances;
use crate::price;

/// What renting is called in failures.
const OPERATION: &str = "create instance";

/// The status an offer already taken answers with.
const GONE: u16 = 410;

/// The error an offer already taken names.
const NO_SUCH_ASK: &str = "no_such_ask";

/// Rents `offer` under `tag`, returning the created instance at the rate
/// the account is charged for it.
///
/// An offer another renter reached first is [`Provision::OfferGone`], which
/// is ordinary marketplace traffic rather than a fault.
pub(crate) fn create(
    client: &VastClient,
    config: &VastConfig,
    offer: &OfferId,
    tag: &str,
) -> Result<Provision> {
    let answer = client.put(&create_path(offer), &request(config, tag), OPERATION)?;
    // The marketplace states a lost offer both ways: as the status and as
    // the body's error.
    if answer.status == GONE || answer.error() == Some(NO_SUCH_ASK) {
        return Ok(Provision::OfferGone);
    }
    let body = answer.ok(OPERATION)?;
    let Some(contract) = body.get("new_contract").and_then(Value::as_i64) else {
        return Err(Error::Provider(format!(
            "{OPERATION}: the answer names no created instance: {body}"
        )));
    };
    let id = InstanceId(contract.to_string());
    // The rate the marketplace charges for the instance is the instance's
    // own, so it is read from the instance rather than carried over from
    // the offer. A failure here is a failure of the rental: the machine
    // exists, and the tag the caller wrote before this call is what
    // reconciliation destroys it by.
    let Some(row) = instances::show(client, &id.0, OPERATION)? else {
        return Err(Error::Provider(format!(
            "{OPERATION}: the account holds no instance {}",
            id.0
        )));
    };
    // A rate no price can be read from states nothing about what the machine
    // costs, so it is the same answer as a row carrying no rate at all.
    let Some(price) = row.dph_total.and_then(price::per_hour) else {
        return Err(Error::Provider(format!(
            "{OPERATION}: the answer names no rate for instance {}",
            id.0
        )));
    };
    Ok(Provision::Provisioned(Instance { id, price }))
}

/// The path renting `offer`.
fn create_path(offer: &OfferId) -> String {
    format!("/api/v0/asks/{}/", offer.0)
}

/// The create request: the rental's shape from `config`, and `tag` as the
/// instance's label, verbatim. The label is the ledger key reconciliation
/// matches on, so nothing rewrites it.
fn request(config: &VastConfig, tag: &str) -> Value {
    let mut body = json!({
        "image": config.image,
        "disk": config.disk_gb,
        "label": tag,
        "runtype": "ssh",
    });
    if let Some(env) = &config.env {
        body["env"] = json!(env);
    }
    body
}

#[cfg(test)]
mod tests {
    use super::create;
    use crate::client::VastClient;
    use crate::config::VastConfig;
    use crate::test_server::{ScriptedAnswer, TestServer};
    use sima_core::{Error, Result};
    use sima_provider::{InstanceId, OfferId, Price, Provision};

    /// A scripted answer with `status` and `body`.
    fn answer(status: u16, body: &str) -> ScriptedAnswer {
        ScriptedAnswer {
            status,
            body: body.to_string(),
        }
    }

    /// Configuration renting `image` at 64 GB against `base_url`.
    fn config(base_url: &str) -> VastConfig {
        VastConfig {
            base_url: base_url.to_string(),
            api_key: "k-secret".to_string(),
            image: "ghcr.io/owner/sima-worker".to_string(),
            disk_gb: 64,
            env: None,
        }
    }

    /// The offer every test rents.
    fn offer() -> OfferId {
        OfferId("8123456".to_string())
    }

    #[test]
    fn renting_an_offer_sends_the_configured_rental_under_the_tag() -> Result<()> {
        let server = TestServer::new(vec![
            answer(200, r#"{"success": true, "new_contract": 555}"#),
            answer(
                200,
                r#"{"instances": {"id": 555, "label": "sima-tag-0", "dph_total": 0.412}}"#,
            ),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        create(&client, &config(&server.url()), &offer(), "sima-tag-0")?;
        let requests = server.requests();
        assert_eq!(requests[0].method, "PUT");
        assert_eq!(requests[0].path, "/api/v0/asks/8123456/");
        let sent = requests[0].json();
        assert_eq!(sent["image"], "ghcr.io/owner/sima-worker");
        assert_eq!(sent["disk"], 64);
        assert_eq!(sent["label"], "sima-tag-0");
        assert_eq!(sent["runtype"], "ssh");
        assert!(sent.get("env").is_none());
        Ok(())
    }

    #[test]
    fn a_configured_environment_reaches_the_rental() -> Result<()> {
        let server = TestServer::new(vec![
            answer(200, r#"{"success": true, "new_contract": 555}"#),
            answer(
                200,
                r#"{"instances": {"id": 555, "label": "sima-tag-0", "dph_total": 0.412}}"#,
            ),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        let mut config = config(&server.url());
        config.env = Some("-e SIMA_ROLE=worker".to_string());
        create(&client, &config, &offer(), "sima-tag-0")?;
        assert_eq!(
            server.requests()[0].json()["env"],
            "-e SIMA_ROLE=worker".to_string()
        );
        Ok(())
    }

    #[test]
    fn a_created_instance_carries_the_rate_the_instance_is_charged_at() -> Result<()> {
        let server = TestServer::new(vec![
            answer(200, r#"{"success": true, "new_contract": 555}"#),
            answer(
                200,
                r#"{"instances": {"id": 555, "label": "sima-tag-0", "dph_total": 0.5125}}"#,
            ),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        let provision = create(&client, &config(&server.url()), &offer(), "sima-tag-0")?;
        let Provision::Provisioned(instance) = provision else {
            panic!("the offer was available");
        };
        assert_eq!(instance.id, InstanceId("555".to_string()));
        assert_eq!(instance.price, Price(512_500));
        assert_eq!(server.requests()[1].path, "/api/v0/instances/555/");
        Ok(())
    }

    #[test]
    fn an_offer_another_renter_took_is_gone_by_its_status() -> Result<()> {
        let server = TestServer::new(vec![answer(
            410,
            r#"{"success": false, "error": "no_such_ask"}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            create(&client, &config(&server.url()), &offer(), "sima-tag-0")?,
            Provision::OfferGone
        ));
        Ok(())
    }

    #[test]
    fn an_offer_the_body_names_as_taken_is_gone_whatever_the_status() -> Result<()> {
        let server = TestServer::new(vec![answer(
            200,
            r#"{"success": false, "error": "no_such_ask"}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            create(&client, &config(&server.url()), &offer(), "sima-tag-0")?,
            Provision::OfferGone
        ));
        Ok(())
    }

    #[test]
    fn a_rejected_rental_reaches_the_caller_as_the_api_named_it() {
        let server = TestServer::new(vec![answer(
            400,
            r#"{"success": false, "error": "insufficient_credit"}"#,
        )]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            create(&client, &config(&server.url()), &offer(), "sima-tag-0"),
            Err(Error::Provider(message))
                if message == "create instance: HTTP 400: insufficient_credit"
        ));
    }

    #[test]
    fn an_answer_naming_no_instance_is_a_provider_error() {
        let server = TestServer::new(vec![answer(200, r#"{"success": true}"#)]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            create(&client, &config(&server.url()), &offer(), "sima-tag-0"),
            Err(Error::Provider(message))
                if message.starts_with("create instance: the answer names no created instance")
        ));
    }

    #[test]
    fn a_rate_the_follow_up_fetch_never_answers_is_a_provider_error() {
        let server = TestServer::new(vec![
            answer(200, r#"{"success": true, "new_contract": 555}"#),
            answer(500, r#"{"success": false, "error": "internal"}"#),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            create(&client, &config(&server.url()), &offer(), "sima-tag-0"),
            Err(Error::Provider(message))
                if message == "create instance: HTTP 500: internal"
        ));
    }

    #[test]
    fn a_created_instance_reporting_no_rate_is_a_provider_error() {
        // The rate is the point of the follow-up fetch, so the one path
        // that consumes it is the one path that demands it.
        let server = TestServer::new(vec![
            answer(200, r#"{"success": true, "new_contract": 555}"#),
            answer(200, r#"{"instances": {"id": 555, "label": "sima-tag-0"}}"#),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            create(&client, &config(&server.url()), &offer(), "sima-tag-0"),
            Err(Error::Provider(message))
                if message == "create instance: the answer names no rate for instance 555"
        ));
    }

    #[test]
    fn a_created_instance_reporting_an_anomalous_rate_is_a_provider_error() {
        // A rate no price can be read from is as good as no rate at all: the
        // rental fails, and the tag-keyed record recovers the machine.
        let server = TestServer::new(vec![
            answer(200, r#"{"success": true, "new_contract": 555}"#),
            answer(
                200,
                r#"{"instances": {"id": 555, "label": "sima-tag-0", "dph_total": -0.4}}"#,
            ),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            create(&client, &config(&server.url()), &offer(), "sima-tag-0"),
            Err(Error::Provider(message))
                if message == "create instance: the answer names no rate for instance 555"
        ));
    }

    #[test]
    fn a_created_instance_the_account_does_not_hold_is_a_provider_error() {
        let server = TestServer::new(vec![
            answer(200, r#"{"success": true, "new_contract": 555}"#),
            answer(404, r#"{"success": false, "error": "not_found"}"#),
        ]);
        let client = VastClient::new(&server.url(), "k-secret");
        assert!(matches!(
            create(&client, &config(&server.url()), &offer(), "sima-tag-0"),
            Err(Error::Provider(message))
                if message == "create instance: the account holds no instance 555"
        ));
    }
}
