use crate::core::{EitherSide, Nameplate};
use crate::server_messages::{EncryptedMessage, ServerMessage};
use std::collections::HashSet;

pub type BroadcastSender = async_broadcast::Sender<EncryptedMessage>;
pub type BroadcastReceiver = async_broadcast::Receiver<EncryptedMessage>;

pub(crate) struct ClaimedMailbox {
    nameplate: Option<Nameplate>,
    clients: HashSet<EitherSide>,
    broadcast_sender: BroadcastSender,
    broadcast_receiver: BroadcastReceiver,
    creation_time: std::time::Instant,
    last_activity: std::time::Instant,
}

impl ClaimedMailbox {
    pub fn new(nameplate: Option<Nameplate>) -> Self {
        let now = std::time::Instant::now();
        let mut clients = HashSet::with_capacity(2);

        let (broadcast_sender, broadcast_receiver) = async_broadcast::broadcast(1024);

        Self {
            nameplate,
            clients,
            broadcast_sender,
            broadcast_receiver,
            creation_time: now,
            last_activity: now,
        }
    }

    pub fn nameplate(&self) -> Option<&Nameplate> {
        self.nameplate.as_ref()
    }

    pub fn add_client(&mut self, client_id: EitherSide) -> bool {
        if self.is_full() {
            false
        } else {
            self.clients.insert(client_id)
        }
    }

    pub fn remove_client(&mut self, client_id: &EitherSide) -> bool {
        self.clients.remove(client_id)
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.clients.len() >= 2
    }

    pub fn should_cleanup(&self) -> bool {
        self.is_empty()
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn has_client(&self, client_id: &EitherSide) -> bool {
        self.clients.contains(client_id)
    }

    pub async fn close_mailbox(&mut self) {
        self.broadcast_receiver.close();
        self.broadcast_sender.close();
    }

    /// This will clone the receiver that is stored in this mailbox.
    /// As the stored receiver is never read, all messages will be retained until the mailbox is
    /// closed. They will be replayed for each new connection to the mailbox.
    pub fn new_broadcast_receiver(&self) -> BroadcastReceiver {
        self.broadcast_receiver.clone()
    }

    pub fn broadcast_sender(&self) -> BroadcastSender {
        self.broadcast_sender.clone()
    }

    pub fn creation_time(&self) -> &std::time::Instant {
        &self.creation_time
    }

    pub fn last_activity(&self) -> &std::time::Instant {
        &self.last_activity
    }

    pub fn update_last_activity(&mut self) {
        self.last_activity = std::time::Instant::now();
    }
}
