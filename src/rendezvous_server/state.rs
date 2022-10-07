use crate::core::AppID;
use crate::core::{EitherSide, Mailbox, Nameplate};
use crate::rendezvous_server::mailbox::ClaimedMailbox;
use crate::rendezvous_server::nameplate::ClaimedNameplate;
use derive_more::Deref;
use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use rand::distributions::DistString;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

// Limit of max open nameplate allocations
const MAX_NAMEPLATES: usize = 1024;

// Two hour old nameplate claims will get deleted
const MAX_NAMEPLATE_CLAIM_TIME: std::time::Duration = Duration::from_secs(60 * 60 * 2);

// Max mailbox open time is 3 days
const MAX_MAILBOX_OPEN_TIME: std::time::Duration = Duration::from_secs(60 * 60 * 24 * 3);

// Maximum time a mailbox is left open without any traffic
const MAX_MAILBOX_IDLE_TIME: std::time::Duration = Duration::from_secs(60 * 60 * 2);

#[derive(Default)]
pub(crate) struct RendezvousServerApp {
    mailboxes: HashMap<Mailbox, ClaimedMailbox>,
    allocations: HashMap<Nameplate, ClaimedNameplate>,
}

impl RendezvousServerApp {
    fn generate_mailbox_id(&self) -> Mailbox {
        let mut char_count = 4;
        loop {
            for _ in 0..10 {
                let mailbox = Mailbox(
                    rand::distributions::Alphanumeric
                        .sample_string(&mut rand::thread_rng(), char_count),
                );
                if !self.mailboxes.contains_key(&mailbox.clone()) {
                    return mailbox;
                }
            }

            char_count *= 2;
        }
    }

    pub fn allocate_nameplate(&mut self, side: &EitherSide) -> Option<Nameplate> {
        if self.allocations.len() + self.mailboxes.len() >= MAX_NAMEPLATES {
            // Sorry, we are full at the moment
            return None;
        }

        for key in 1..MAX_NAMEPLATES {
            if self
                .claim_nameplate(&Nameplate(key.to_string()), &side)
                .is_some()
            {
                return Some(Nameplate(key.to_string()));
            }
        }

        // MAX_ALLOCATIONS reached
        None
    }

    pub fn claim_nameplate(&mut self, nameplate: &Nameplate, side: &EitherSide) -> Option<Mailbox> {
        let mut claimed_nameplate =
            if let Some(claimed_nameplate) = self.allocations.get_mut(&nameplate) {
                claimed_nameplate
            } else {
                let mailbox_id = self.generate_mailbox_id();
                self.add_or_get_mailbox(&mailbox_id, Some(&nameplate));

                let claimed_nameplate = ClaimedNameplate::new(mailbox_id);
                self.allocations
                    .entry(nameplate.clone())
                    .or_insert(claimed_nameplate)
            };

        claimed_nameplate.add_client(side.clone());
        Some(claimed_nameplate.mailbox().clone())
    }

    pub fn release_nameplate(&mut self, nameplate_id: &Nameplate, client_id: &EitherSide) -> bool {
        if let Some(claimed_nameplate) = self.allocations.get_mut(nameplate_id) {
            if claimed_nameplate.remove_client(client_id) {
                if claimed_nameplate.is_empty() {
                    // Cleanup nameplate
                    self.allocations.remove(nameplate_id);
                }

                return true;
            }
        }

        false
    }

    fn add_or_get_mailbox(
        &mut self,
        mailbox: &Mailbox,
        nameplate: Option<&Nameplate>,
    ) -> &mut ClaimedMailbox {
        self.mailboxes
            .entry(mailbox.clone())
            .or_insert_with(|| ClaimedMailbox::new(nameplate.cloned()))
    }

    pub fn open_mailbox(
        &mut self,
        mailbox_id: &Mailbox,
        client_id: &EitherSide,
    ) -> Option<Mailbox> {
        let mut mailbox = self.add_or_get_mailbox(mailbox_id, None);
        if mailbox.add_client(client_id.clone()) {
            Some(mailbox_id.clone())
        } else {
            None
        }
    }

    pub fn close_mailbox(&mut self, mailbox_id: &Mailbox, client_id: &EitherSide) -> bool {
        println!("Closing mailbox {} for client {}", mailbox_id, client_id);

        let mut claimed_mailbox = self.mailboxes.get_mut(mailbox_id);
        if let Some(claimed_mailbox) = claimed_mailbox {
            if claimed_mailbox.has_client(client_id) {
                claimed_mailbox.remove_client(client_id);
                if claimed_mailbox.is_empty() {
                    println!("Removing mailbox {}", mailbox_id);
                    self.mailboxes.remove(mailbox_id);
                }

                return true;
            }
        }

        false
    }

    pub fn cleanup_allocations(&mut self) {
        self.allocations.retain(|_nameplate, claimed_nameplate| {
            if claimed_nameplate.is_empty() {
                return false;
            }

            // Two hours is maximum nameplate claim time
            if claimed_nameplate.creation_time().elapsed() > MAX_NAMEPLATE_CLAIM_TIME {
                true
            } else {
                false
            }
        });
    }

    pub fn cleanup_mailboxes(&mut self) {
        self.mailboxes.retain(|nameplate, mailbox| {
            if mailbox.should_cleanup() {
                println!("Removed mailbox {}: Empty", nameplate);
                return false;
            }

            let creation_duration = MAX_MAILBOX_OPEN_TIME;
            if mailbox.creation_time().elapsed() > creation_duration {
                println!("Removed mailbox {}: Too old", nameplate);
                return false;
            }

            if mailbox.last_activity().elapsed() > MAX_MAILBOX_IDLE_TIME {
                println!("Removed mailbox {}: No recent activity", nameplate);
                false
            } else {
                true
            }
        })
    }

    pub fn mailbox_has_client(&self, nameplate: &Mailbox, client_id: &EitherSide) -> bool {
        if let Some(mailbox) = self.mailboxes.get(nameplate) {
            mailbox.has_client(client_id)
        } else {
            false
        }
    }

    pub fn mailboxes(&self) -> &HashMap<Mailbox, ClaimedMailbox> {
        &self.mailboxes
    }

    pub fn mailboxes_mut(&mut self) -> &mut HashMap<Mailbox, ClaimedMailbox> {
        &mut self.mailboxes
    }

    pub fn mailbox(&self, mailbox_id: &Mailbox) -> Option<&ClaimedMailbox> {
        self.mailboxes.get(mailbox_id)
    }

    pub fn mailbox_mut(&mut self, mailbox_id: &Mailbox) -> Option<&mut ClaimedMailbox> {
        self.mailboxes.get_mut(mailbox_id)
    }

    pub fn allocations_mut(&mut self) -> &mut HashMap<Nameplate, ClaimedNameplate> {
        &mut self.allocations
    }
}

#[derive(Default)]
pub(crate) struct RendezvousServerStateInner {
    apps: HashMap<AppID, RendezvousServerApp>,
}

impl RendezvousServerStateInner {
    pub fn app(&self, app_id: &AppID) -> Option<&RendezvousServerApp> {
        self.apps.get(app_id)
    }

    /// Creates app if it doesn't exist yet
    pub fn app_mut(&mut self, app_id: &AppID) -> &mut RendezvousServerApp {
        self.apps.entry(app_id.clone()).or_insert_with(|| {
            println!("Spawning app with id: {app_id}");
            RendezvousServerApp::default()
        })
    }

    pub fn cleanup_allocations(&mut self) {
        for app in self.apps.values_mut() {
            app.cleanup_allocations();
        }
    }

    pub fn cleanup_mailboxes(&mut self) {
        for app in self.apps.values_mut() {
            app.cleanup_mailboxes();
        }
    }

    pub fn cleanup_apps(&mut self) {
        self.apps.retain(|key, app| {
            if app.mailboxes.is_empty() && app.allocations.is_empty() {
                println!("Cleaning up app with id {key}");
                false
            } else {
                true
            }
        });
    }

    pub fn cleanup_all(&mut self) {
        self.cleanup_allocations();
        self.cleanup_mailboxes();
        self.cleanup_apps();
    }
}

#[derive(Deref, Clone, Default)]
pub(crate) struct RendezvousServerState(Arc<RwLock<RendezvousServerStateInner>>);

impl RendezvousServerState {}
