use crate::rendezvous_server::{EitherSide, Mailbox};
use std::collections::HashMap;

pub(crate) struct ClaimedNameplateSide {
    creation_date: std::time::Instant,
    claimed: bool,
}

impl ClaimedNameplateSide {
    pub fn new() -> Self {
        Self {
            creation_date: std::time::Instant::now(),
            claimed: true,
        }
    }

    pub fn creation_date(&self) -> &std::time::Instant {
        &self.creation_date
    }

    pub fn release(&mut self) {
        self.claimed = false;
    }

    pub fn is_claimed(&self) -> bool {
        self.claimed
    }
}

pub(crate) struct ClaimedNameplate {
    mailbox: Mailbox,
    creation_time: std::time::Instant,
    clients: HashMap<EitherSide, ClaimedNameplateSide>,
}

impl ClaimedNameplate {
    pub fn new(mailbox: Mailbox) -> Self {
        Self {
            mailbox,
            creation_time: std::time::Instant::now(),
            clients: HashMap::with_capacity(2),
        }
    }

    pub fn add_client(&mut self, side: EitherSide) -> bool {
        if self.clients.len() >= 2 || self.clients.contains_key(&side) {
            false
        } else {
            self.clients
                .insert(side.clone(), ClaimedNameplateSide::new());
            true
        }
    }

    pub fn remove_client(&mut self, side: &EitherSide) -> bool {
        self.clients.remove(side).is_some()
    }

    pub fn client_mut(&mut self, side: &EitherSide) -> Option<&mut ClaimedNameplateSide> {
        self.clients.get_mut(side)
    }

    pub fn has_client(&self, client_id: &EitherSide) -> bool {
        self.clients.contains_key(client_id)
    }

    pub fn mailbox(&self) -> &Mailbox {
        &self.mailbox
    }

    pub fn is_full(&self) -> bool {
        self.clients.len() >= 2
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub fn creation_time(&self) -> &std::time::Instant {
        &self.creation_time
    }
}
