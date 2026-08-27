//! [`StubProvider`]: an in-memory marketplace standing in for a backend.
//!
//! The stub is a public type so that consumers above this crate can test
//! their acquisition paths against it. It honors every obligation the
//! [`Provider`] contract states, and its scripted departures — a lost
//! offer, a machine that never comes up, a failing API — are the cases a
//! real marketplace produces and callers must handle.

use std::collections::HashMap;
use std::sync::Mutex;

use sima_core::{Error, Result};

use crate::offer::{Offer, OfferId, Price};
use crate::provider::{
    Instance, InstanceId, InstanceStatus, Provider, Provision, Reachability, SshEndpoint,
    TaggedInstance,
};

/// The stub's scripted behavior and its market state, together under one
/// lock so the `&self` contract holds under concurrent use.
#[derive(Default)]
struct StubState {
    /// The marketplace this stub lists.
    offers: Vec<Offer>,
    /// Offers already rented; a second request for one is `OfferGone`. A
    /// rented offer stays here for the stub's life, so it never returns to
    /// the marketplace. A real marketplace relists a machine once its renter
    /// releases it, and the acquisition loop visits each offer at most once,
    /// so holding an offer taken is the whole marketplace behavior the
    /// contract exposes.
    taken: Vec<OfferId>,
    /// Offers that answer `OfferGone` however often they are requested.
    lost: Vec<OfferId>,
    /// Offers whose instances stay `Provisioning` forever.
    stalling: Vec<OfferId>,
    /// Offers whose instances answer `Provisioning` for a scripted number of
    /// status calls and are gone from then on.
    vanishing: Vec<(OfferId, u32)>,
    /// The rate created instances are charged at, when it departs from the
    /// offer's.
    instance_price: Option<Price>,
    /// Status calls an instance answers `Provisioning` before it is ready.
    ready_after: u32,
    /// The message `offers` fails with, when scripted to fail.
    offers_failure: Option<String>,
    /// The message `provision` fails with, when scripted to fail.
    provision_failure: Option<String>,
    /// The message `instance` fails with, when scripted to fail.
    instance_failure: Option<String>,
    /// Every instance the stub ever created or was seeded with, by id.
    instances: HashMap<String, StubInstance>,
    /// The endpoint every ready instance reports, when the stub was pointed at
    /// a real one. `None` fabricates an endpoint that names no machine.
    endpoint: Option<SshEndpoint>,
    /// Instance ids passed to `destroy`, in call order.
    destroyed: Vec<InstanceId>,
    /// Source of the next instance id.
    next_id: u64,
}

/// One instance the stub holds.
struct StubInstance {
    /// The tag it was created under.
    tag: String,
    /// The rate it is charged at, `None` for an instance the stub was seeded
    /// with, which stands in for a machine this process never created.
    price: Option<Price>,
    /// Whether the account listing carries it. An instance the listing
    /// omits is still answered for by id, which is what a marketplace row
    /// carrying no tag looks like.
    listed: bool,
    /// Whether it stays `Provisioning` however long it is polled.
    stalling: bool,
    /// The status call it is gone from, `None` for an instance the account
    /// keeps holding.
    gone_after: Option<u32>,
    /// Status calls it has answered so far.
    polls: u32,
    /// Whether `destroy` has taken it.
    destroyed: bool,
}

/// The id this backend answers to, and the one a ledger record written by a
/// stub rental carries.
pub const STUB_PROVIDER_ID: &str = "stub";

/// An in-memory provider over a fixed marketplace.
pub struct StubProvider {
    state: Mutex<StubState>,
}

impl StubProvider {
    /// A stub listing `offers`, provisioning each into an instance that is
    /// ready at its first status call.
    pub fn new(offers: Vec<Offer>) -> StubProvider {
        StubProvider {
            state: Mutex::new(StubState {
                offers,
                ..StubState::default()
            }),
        }
    }

    /// Scripts `offer` as taken by another renter: provisioning it answers
    /// [`Provision::OfferGone`].
    pub fn gone_at_provision(self, offer: OfferId) -> StubProvider {
        self.edit(|state| state.lost.push(offer));
        self
    }

    /// Scripts created instances to be charged at `price` whatever the
    /// offer listed, which is what a marketplace billing the machine at its
    /// own rate looks like.
    pub fn charging_instances_at(self, price: Price) -> StubProvider {
        self.edit(|state| state.instance_price = Some(price));
        self
    }

    /// Points every instance at a machine that is really there, so a caller
    /// reaches it over ssh as it reaches a rented one.
    ///
    /// Without it the stub fabricates an endpoint naming no machine, and its
    /// [`Provider::reachability`] says so: the in-process path spawns on this
    /// machine instead of connecting to a host that does not exist.
    pub fn endpoint(self, host: &str, port: u16, user: &str) -> StubProvider {
        self.edit(|state| {
            state.endpoint = Some(SshEndpoint {
                host: host.to_string(),
                port,
                user: user.to_string(),
            })
        });
        self
    }

    /// Scripts instances to answer `Provisioning` for `polls` status calls
    /// before they are ready.
    pub fn ready_after(self, polls: u32) -> StubProvider {
        self.edit(|state| state.ready_after = polls);
        self
    }

    /// Scripts `offer` to provision into an instance that never becomes
    /// ready.
    pub fn never_ready(self, offer: OfferId) -> StubProvider {
        self.edit(|state| state.stalling.push(offer));
        self
    }

    /// Scripts `offer` to provision into an instance that answers
    /// `Provisioning` for `polls` status calls and is gone from then on,
    /// which is what a machine the marketplace withdraws while it is coming
    /// up looks like.
    pub fn gone_after(self, offer: OfferId, polls: u32) -> StubProvider {
        self.edit(|state| state.vanishing.push((offer, polls)));
        self
    }

    /// Scripts [`Provider::offers`] to fail with `message`.
    pub fn failing_offers(self, message: &str) -> StubProvider {
        self.edit(|state| state.offers_failure = Some(message.to_string()));
        self
    }

    /// Scripts [`Provider::provision`] to fail with `message`.
    pub fn failing_provision(self, message: &str) -> StubProvider {
        self.edit(|state| state.provision_failure = Some(message.to_string()));
        self
    }

    /// Scripts [`Provider::instance`] to fail with `message`.
    pub fn failing_instance(self, message: &str) -> StubProvider {
        self.edit(|state| state.instance_failure = Some(message.to_string()));
        self
    }

    /// Seeds an instance the account already holds under `tag`, standing in
    /// for a machine an earlier process rented.
    pub fn with_instance(self, id: InstanceId, tag: &str) -> StubProvider {
        self.seed(id, tag, true)
    }

    /// Seeds an instance the account holds and the listing omits, which is
    /// what a marketplace row carrying no tag looks like: it answers by id
    /// and appears in no scan.
    pub fn with_unlisted_instance(self, id: InstanceId, tag: &str) -> StubProvider {
        self.seed(id, tag, false)
    }

    /// Seeds an instance under `tag`, carried by the account listing where
    /// `listed`.
    fn seed(self, id: InstanceId, tag: &str, listed: bool) -> StubProvider {
        self.edit(|state| {
            state.instances.insert(
                id.0,
                StubInstance {
                    tag: tag.to_string(),
                    price: None,
                    listed,
                    stalling: false,
                    gone_after: None,
                    polls: 0,
                    destroyed: false,
                },
            );
        });
        self
    }

    /// Every instance id passed to [`Provider::destroy`], in call order.
    pub fn destroyed(&self) -> Vec<InstanceId> {
        self.read(|state| state.destroyed.clone())
    }

    /// The instances the stub still holds, in creation order.
    pub fn live(&self) -> Vec<InstanceId> {
        self.read(|state| {
            let mut live: Vec<InstanceId> = state
                .instances
                .iter()
                .filter(|(_, instance)| !instance.destroyed)
                .map(|(id, _)| InstanceId(id.clone()))
                .collect();
            live.sort_by(|a, b| a.0.cmp(&b.0));
            live
        })
    }

    /// Applies `edit` to the state. The lock is held by no one else while a
    /// stub is being configured or read.
    fn edit(&self, edit: impl FnOnce(&mut StubState)) {
        edit(&mut self.state.lock().expect("stub state lock"));
    }

    /// Reads `read` off the state.
    fn read<T>(&self, read: impl FnOnce(&StubState) -> T) -> T {
        read(&self.state.lock().expect("stub state lock"))
    }
}

impl Provider for StubProvider {
    fn id(&self) -> &'static str {
        STUB_PROVIDER_ID
    }

    fn offers(&self) -> Result<Vec<Offer>> {
        self.read(|state| match &state.offers_failure {
            Some(message) => Err(Error::Provider(message.clone())),
            None => Ok(state.offers.clone()),
        })
    }

    fn provision(&self, offer: &OfferId, tag: &str) -> Result<Provision> {
        let mut state = self.state.lock().expect("stub state lock");
        if let Some(message) = &state.provision_failure {
            return Err(Error::Provider(message.clone()));
        }
        // One machine, one renter: an offer scripted as lost, and one this
        // stub already rented out, are both gone.
        if state.lost.contains(offer) || state.taken.contains(offer) {
            return Ok(Provision::OfferGone);
        }
        let Some(listed) = state.offers.iter().find(|listed| listed.id == *offer) else {
            return Ok(Provision::OfferGone);
        };
        let price = state.instance_price.unwrap_or(listed.price);
        let stalling = state.stalling.contains(offer);
        let gone_after = state
            .vanishing
            .iter()
            .find(|(scripted, _)| scripted == offer)
            .map(|(_, polls)| *polls);
        let id = InstanceId(format!("stub-{}", state.next_id));
        state.next_id += 1;
        state.taken.push(offer.clone());
        state.instances.insert(
            id.0.clone(),
            StubInstance {
                tag: tag.to_string(),
                price: Some(price),
                listed: true,
                stalling,
                gone_after,
                polls: 0,
                destroyed: false,
            },
        );
        Ok(Provision::Provisioned(Instance { id, price }))
    }

    fn instance(&self, id: &InstanceId) -> Result<InstanceStatus> {
        let mut state = self.state.lock().expect("stub state lock");
        if let Some(message) = &state.instance_failure {
            return Err(Error::Provider(message.clone()));
        }
        let ready_after = state.ready_after;
        let Some(instance) = state.instances.get_mut(&id.0) else {
            return Ok(InstanceStatus::Gone);
        };
        if instance.destroyed {
            return Ok(InstanceStatus::Gone);
        }
        // A machine scripted to vanish counts the calls it answers itself:
        // it comes up until the scripted one, and the account holds it no
        // more from there.
        if let Some(gone_after) = instance.gone_after {
            if instance.polls >= gone_after {
                return Ok(InstanceStatus::Gone);
            }
            instance.polls += 1;
            return Ok(InstanceStatus::Provisioning);
        }
        if instance.stalling {
            return Ok(InstanceStatus::Provisioning);
        }
        if instance.polls < ready_after {
            instance.polls += 1;
            return Ok(InstanceStatus::Provisioning);
        }
        let reported = state
            .endpoint
            .clone()
            .unwrap_or_else(|| fabricated_endpoint(id));
        Ok(InstanceStatus::Ready(reported))
    }

    fn instances(&self) -> Result<Vec<TaggedInstance>> {
        self.read(|state| {
            let mut held: Vec<TaggedInstance> = state
                .instances
                .iter()
                .filter(|(_, instance)| !instance.destroyed && instance.listed)
                .map(|(id, instance)| TaggedInstance {
                    id: InstanceId(id.clone()),
                    tag: instance.tag.clone(),
                    // A seeded instance carries no rate of its own, and
                    // takes the scripted one where the stub charges at it.
                    price: instance.price.or(state.instance_price),
                })
                .collect();
            held.sort_by(|a, b| a.id.0.cmp(&b.id.0));
            Ok(held)
        })
    }

    fn destroy(&self, id: &InstanceId) -> Result<()> {
        self.edit(|state| {
            state.destroyed.push(id.clone());
            if let Some(instance) = state.instances.get_mut(&id.0) {
                instance.destroyed = true;
            }
        });
        Ok(())
    }

    /// Over ssh exactly when this stub was pointed at a machine that is really
    /// there; otherwise there is nothing to connect to.
    fn reachability(&self) -> Reachability {
        self.read(|state| match state.endpoint {
            Some(_) => Reachability::Ssh,
            None => Reachability::Local,
        })
    }
}

/// The endpoint a stub instance reports when the stub was pointed at no real
/// one: a host that names no machine, which is why such a stub is reached
/// locally.
fn fabricated_endpoint(id: &InstanceId) -> SshEndpoint {
    SshEndpoint {
        host: format!("stub-{}", id.0),
        port: 22,
        user: "root".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::StubProvider;
    use crate::offer::Price;
    use crate::provider::{
        InstanceId, InstanceStatus, Provider, Provision, Reachability, SshEndpoint,
    };
    use crate::testutil::stub_offer;
    use sima_core::{Error, Result};

    /// The instance a successful provision produced.
    fn provisioned(provision: Provision) -> InstanceId {
        match provision {
            Provision::Provisioned(instance) => instance.id,
            Provision::OfferGone => panic!("the offer was expected to be available"),
        }
    }

    #[test]
    fn provisioning_creates_an_instance_at_the_offers_price_under_the_tag() -> Result<()> {
        let stub = StubProvider::new(vec![stub_offer("cheap", 100_000)]);
        let provision = stub.provision(&stub_offer("cheap", 100_000).id, "sima-tag-0")?;
        let Provision::Provisioned(instance) = provision else {
            panic!("the listed offer must provision");
        };
        assert_eq!(instance.price, stub_offer("cheap", 100_000).price);
        let held = stub.instances()?;
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].id, instance.id);
        assert_eq!(held[0].tag, "sima-tag-0");
        Ok(())
    }

    #[test]
    fn a_scripted_instance_rate_replaces_the_offers() -> Result<()> {
        let offer = stub_offer("listed", 100_000);
        let stub = StubProvider::new(vec![offer.clone()]).charging_instances_at(Price(250_000));
        let Provision::Provisioned(instance) = stub.provision(&offer.id, "sima-tag-0")? else {
            panic!("the listed offer must provision");
        };
        assert_eq!(instance.price, Price(250_000));
        Ok(())
    }

    #[test]
    fn an_offer_scripted_as_lost_provisions_to_offer_gone() -> Result<()> {
        let offer = stub_offer("lost", 100_000);
        let stub = StubProvider::new(vec![offer.clone()]).gone_at_provision(offer.id.clone());
        assert!(matches!(
            stub.provision(&offer.id, "sima-tag-0")?,
            Provision::OfferGone
        ));
        assert!(stub.instances()?.is_empty());
        Ok(())
    }

    #[test]
    fn an_offer_rented_once_is_gone_the_second_time() -> Result<()> {
        let offer = stub_offer("single", 100_000);
        let stub = StubProvider::new(vec![offer.clone()]);
        stub.provision(&offer.id, "sima-tag-0")?;
        assert!(matches!(
            stub.provision(&offer.id, "sima-tag-1")?,
            Provision::OfferGone
        ));
        Ok(())
    }

    #[test]
    fn an_unlisted_offer_provisions_to_offer_gone() -> Result<()> {
        let stub = StubProvider::new(Vec::new());
        assert!(matches!(
            stub.provision(&stub_offer("absent", 1).id, "sima-tag-0")?,
            Provision::OfferGone
        ));
        Ok(())
    }

    #[test]
    fn an_instance_is_ready_after_the_scripted_number_of_polls() -> Result<()> {
        let offer = stub_offer("slow", 100_000);
        let stub = StubProvider::new(vec![offer.clone()]).ready_after(2);
        let id = provisioned(stub.provision(&offer.id, "sima-tag-0")?);
        assert_eq!(stub.instance(&id)?, InstanceStatus::Provisioning);
        assert_eq!(stub.instance(&id)?, InstanceStatus::Provisioning);
        assert_eq!(
            stub.instance(&id)?,
            InstanceStatus::Ready(SshEndpoint {
                host: format!("stub-{}", id.0),
                port: 22,
                user: "root".to_string(),
            })
        );
        Ok(())
    }

    #[test]
    fn a_never_ready_offers_instance_stays_provisioning() -> Result<()> {
        let offer = stub_offer("stalling", 100_000);
        let stub = StubProvider::new(vec![offer.clone()]).never_ready(offer.id.clone());
        let id = provisioned(stub.provision(&offer.id, "sima-tag-0")?);
        for _ in 0..5 {
            assert_eq!(stub.instance(&id)?, InstanceStatus::Provisioning);
        }
        Ok(())
    }

    #[test]
    fn a_vanishing_offers_instance_comes_up_until_the_scripted_call() -> Result<()> {
        let offer = stub_offer("withdrawn", 100_000);
        let stub = StubProvider::new(vec![offer.clone()]).gone_after(offer.id.clone(), 2);
        let id = provisioned(stub.provision(&offer.id, "sima-tag-0")?);
        assert_eq!(stub.instance(&id)?, InstanceStatus::Provisioning);
        assert_eq!(stub.instance(&id)?, InstanceStatus::Provisioning);
        // Gone from the scripted call on, however often it is asked for.
        assert_eq!(stub.instance(&id)?, InstanceStatus::Gone);
        assert_eq!(stub.instance(&id)?, InstanceStatus::Gone);
        Ok(())
    }

    #[test]
    fn destroying_an_instance_takes_it_and_a_second_destroy_is_ok() -> Result<()> {
        let offer = stub_offer("rented", 100_000);
        let stub = StubProvider::new(vec![offer.clone()]);
        let id = provisioned(stub.provision(&offer.id, "sima-tag-0")?);
        stub.destroy(&id)?;
        assert_eq!(stub.instance(&id)?, InstanceStatus::Gone);
        assert!(stub.instances()?.is_empty());
        assert!(stub.live().is_empty());
        // Idempotent: destroying an instance already gone is success.
        stub.destroy(&id)?;
        assert_eq!(stub.destroyed(), vec![id.clone(), id]);
        Ok(())
    }

    #[test]
    fn an_unknown_instance_is_gone() -> Result<()> {
        let stub = StubProvider::new(Vec::new());
        assert_eq!(
            stub.instance(&InstanceId("never-existed".to_string()))?,
            InstanceStatus::Gone
        );
        Ok(())
    }

    #[test]
    fn a_seeded_instance_is_held_under_its_tag_and_states_no_rate() -> Result<()> {
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-7".to_string()), "sima-tag-7");
        let held = stub.instances()?;
        assert_eq!(held.len(), 1);
        assert_eq!(held[0].id, InstanceId("i-7".to_string()));
        assert_eq!(held[0].tag, "sima-tag-7");
        // Nothing scripted a rate, which is a marketplace listing that
        // reports none.
        assert_eq!(held[0].price, None);
        Ok(())
    }

    #[test]
    fn a_provisioned_instance_lists_the_rate_it_was_created_at() -> Result<()> {
        let offer = stub_offer("listed", 100_000);
        let stub = StubProvider::new(vec![offer.clone()]);
        stub.provision(&offer.id, "sima-tag-0")?;
        assert_eq!(stub.instances()?[0].price, Some(Price(100_000)));
        Ok(())
    }

    #[test]
    fn a_scripted_instance_rate_is_what_the_listing_states() -> Result<()> {
        let offer = stub_offer("listed", 100_000);
        let stub = StubProvider::new(vec![offer.clone()])
            .charging_instances_at(Price(250_000))
            .with_instance(InstanceId("i-7".to_string()), "sima-tag-7");
        stub.provision(&offer.id, "sima-tag-8")?;
        // The rate the stub charges covers the machines it creates and the
        // ones it was seeded with, which stand in for an earlier process's.
        let rates: Vec<Option<Price>> = stub
            .instances()?
            .into_iter()
            .map(|instance| instance.price)
            .collect();
        assert_eq!(rates, vec![Some(Price(250_000)), Some(Price(250_000))]);
        Ok(())
    }

    #[test]
    fn an_unlisted_instance_answers_by_id_and_appears_in_no_scan() -> Result<()> {
        let id = InstanceId("i-8".to_string());
        let stub = StubProvider::new(Vec::new()).with_unlisted_instance(id.clone(), "sima-tag-8");
        assert!(stub.instances()?.is_empty());
        // The machine is held all the same: it answers for its id, and it is
        // still there to destroy.
        assert!(matches!(stub.instance(&id)?, InstanceStatus::Ready(_)));
        assert_eq!(stub.live(), vec![id.clone()]);
        stub.destroy(&id)?;
        assert_eq!(stub.instance(&id)?, InstanceStatus::Gone);
        Ok(())
    }

    #[test]
    fn a_scripted_api_failure_surfaces_from_offers_and_provision() {
        let listing = StubProvider::new(Vec::new()).failing_offers("list offers: 503");
        assert!(matches!(
            listing.offers(),
            Err(Error::Provider(message)) if message == "list offers: 503"
        ));
        let offer = stub_offer("any", 100_000);
        let renting =
            StubProvider::new(vec![offer.clone()]).failing_provision("create instance: 429");
        assert!(matches!(
            renting.provision(&offer.id, "sima-tag-0"),
            Err(Error::Provider(message)) if message == "create instance: 429"
        ));
    }

    #[test]
    fn a_stub_pointed_at_no_machine_is_reached_locally() {
        // The endpoint it fabricates names a host nothing resolves, so there is
        // nothing to connect to and the caller spawns here instead.
        let stub = StubProvider::new(Vec::new()).with_instance(InstanceId("i-1".into()), "tag");
        assert_eq!(stub.reachability(), Reachability::Local);
        let InstanceStatus::Ready(endpoint) =
            stub.instance(&InstanceId("i-1".into())).expect("ready")
        else {
            panic!("a seeded instance is ready");
        };
        assert_eq!(endpoint.host, "stub-i-1");
    }

    #[test]
    fn a_stub_pointed_at_a_machine_reports_it_and_is_reached_over_ssh() {
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-1".into()), "tag")
            .endpoint("127.0.0.1", 41022, "tester");
        assert_eq!(stub.reachability(), Reachability::Ssh);
        let InstanceStatus::Ready(endpoint) =
            stub.instance(&InstanceId("i-1".into())).expect("ready")
        else {
            panic!("a seeded instance is ready");
        };
        assert_eq!(
            endpoint,
            SshEndpoint {
                host: "127.0.0.1".to_string(),
                port: 41022,
                user: "tester".to_string(),
            }
        );
    }

    #[test]
    fn a_scripted_api_failure_surfaces_from_the_status_call() {
        let stub = StubProvider::new(Vec::new())
            .with_instance(InstanceId("i-9".to_string()), "sima-tag-9")
            .failing_instance("show instance: 503");
        assert!(matches!(
            stub.instance(&InstanceId("i-9".to_string())),
            Err(Error::Provider(message)) if message == "show instance: 503"
        ));
    }
}
