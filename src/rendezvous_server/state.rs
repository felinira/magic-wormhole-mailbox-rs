use crate::core::AppID;
use crate::core::{EitherSide, Mailbox, Nameplate};
use crate::rendezvous_server::mailbox::{ClaimedMailbox, MailboxClient};
use derive_more::Deref;
use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// Limit of max open mailboxes
const MAX_MAILBOXES: usize = 1024;

#[derive(Default)]
pub(crate) struct RendezvousServerApp {
    mailboxes: HashMap<Mailbox, ClaimedMailbox>,
    allocations: HashMap<Nameplate, (EitherSide, std::time::Instant)>,
}

impl RendezvousServerApp {
    pub fn try_claim(&mut self, nameplate: &Nameplate, client_id: EitherSide) -> Option<Mailbox> {
        if self.mailboxes.len() > MAX_MAILBOXES {
            // Sorry, no mailboxes are free at the moment
            return None;
        }

        let mailbox_id = Mailbox(nameplate.to_string());

        if self.mailboxes.get(&mailbox_id).is_none() {
            // We check allocations if the mailbox is not open yet
            if let Some((allocated_client_id, _time)) = self.allocations.get(nameplate) {
                if &client_id != allocated_client_id {
                    // This allocation was not for you
                    return None;
                } else {
                    self.allocations.remove(nameplate);
                }
            }

            self.mailboxes
                .insert(mailbox_id.clone(), ClaimedMailbox::new(client_id));
            return Some(mailbox_id);
        } else if !self.mailbox_has_client(&mailbox_id, &client_id) {
            let claimed_mailbox = self.mailboxes.get_mut(&mailbox_id);
            if let Some(claimed_mailbox) = claimed_mailbox {
                if claimed_mailbox.add_client(client_id).is_some() {
                    return Some(mailbox_id);
                }
            }
        }

        None
    }

    pub fn cleanup_allocations(&mut self) {
        self.allocations
            .retain(|_nameplate, (_client_id, time)| time.elapsed() < Duration::from_secs(60));
    }

    pub fn cleanup_mailboxes(&mut self) {
        println!("Cleaning up mailboxes");
        self.mailboxes.retain(|nameplate, mailbox| {
            if mailbox.should_cleanup() {
                println!("Mailbox {} removed", nameplate);
                return false;
            }

            // 72 hours after creation in any case
            let creation_duration = Duration::from_secs(60 * 60 * 72);
            if mailbox.creation_time().elapsed() > creation_duration {
                println!("Mailbox {} removed", nameplate);
                return false;
            }

            let activity_duration = if mailbox.is_full() {
                //24 hours after last message
                Duration::from_secs(60 * 60 * 24)
            } else {
                // 2 hours after last activity if mailbox is not full
                Duration::from_secs(60 * 60 * 2)
            };

            if mailbox.last_activity().elapsed() > activity_duration {
                println!("Mailbox {} removed", nameplate);
                false
            } else {
                true
            }
        })
    }

    pub fn allocate(&mut self, client_id: &EitherSide) -> Option<Nameplate> {
        if self.allocations.len() + self.mailboxes.len() >= MAX_MAILBOXES {
            // Sorry, we are full at the moment
            return None;
        }

        for key in 1..MAX_MAILBOXES {
            let mailbox_id = Mailbox(key.to_string());
            let nameplate = Nameplate(key.to_string());
            if !self.mailboxes.contains_key(&mailbox_id)
                && !self.allocations.contains_key(&nameplate)
            {
                self.allocations.insert(
                    nameplate.clone(),
                    (client_id.clone(), std::time::Instant::now()),
                );
                return Some(nameplate);
            }
        }

        // MAX_ALLOCATIONS reached
        None
    }

    pub fn mailbox_has_client(&self, nameplate: &Mailbox, client_id: &EitherSide) -> bool {
        if let Some(mailbox) = self.mailboxes.get(nameplate) {
            mailbox.has_client(client_id)
        } else {
            false
        }
    }

    pub fn remove_client(&mut self, mailbox_id: &Mailbox, client: &EitherSide) -> bool {
        println!("Removing client {} from mailbox {}", client, mailbox_id);
        let mailbox = self.mailboxes.get_mut(mailbox_id);
        if let Some(mailbox) = mailbox {
            let res = mailbox.remove_client(client);

            if mailbox.is_empty() {
                self.mailboxes.remove(mailbox_id);
            }

            res
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

    pub fn allocations_mut(&mut self) -> &mut HashMap<Nameplate, (EitherSide, std::time::Instant)> {
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
            if app.mailboxes.is_empty() && !app.allocations.is_empty() {
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
