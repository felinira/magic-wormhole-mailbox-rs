use crate::core::EitherSide;
use crate::server_messages::{EncryptedMessage, ServerMessage};

pub type BroadcastSender = async_broadcast::Sender<EncryptedMessage>;
pub type BroadcastReceiver = async_broadcast::Receiver<EncryptedMessage>;

pub(crate) struct MailboxClient {
    id: EitherSide,
    is_open: bool,
    was_released: bool,
}

impl MailboxClient {
    pub fn new(id: EitherSide) -> Self {
        Self {
            id,
            is_open: false,
            was_released: false,
        }
    }

    pub fn id(&self) -> &EitherSide {
        &self.id
    }

    /// Can only open once
    pub fn open(&mut self) -> bool {
        if self.is_open {
            return false;
        }

        if !self.was_released {
            self.is_open = true;
        }

        self.is_open
    }

    pub fn release(&mut self) {
        self.is_open = false;
        self.was_released = true;
    }

    pub fn is_open(&self) -> bool {
        self.is_open
    }

    pub fn should_cleanup(&self) -> bool {
        self.was_released
    }
}

pub(crate) struct ClaimedMailbox {
    clients: Vec<MailboxClient>,
    broadcast_sender: BroadcastSender,
    broadcast_receiver: BroadcastReceiver,
    creation_time: std::time::Instant,
    last_activity: std::time::Instant,
}

impl ClaimedMailbox {
    pub fn new(first_client_id: EitherSide) -> Self {
        let now = std::time::Instant::now();
        let mut clients = Vec::with_capacity(2);
        clients.push(MailboxClient::new(first_client_id));

        let (broadcast_sender, broadcast_receiver) = async_broadcast::broadcast(1024);

        Self {
            clients,
            broadcast_sender,
            broadcast_receiver,
            creation_time: now,
            last_activity: now,
        }
    }

    pub fn add_client(&mut self, client_id: EitherSide) -> Option<&MailboxClient> {
        if self.is_full() {
            None
        } else {
            let client = MailboxClient::new(client_id);
            self.clients.push(client);
            self.clients.last()
        }
    }

    pub fn remove_client(&mut self, client_id: &EitherSide) -> bool {
        if let Some(pos) = self.clients.iter().position(|c| c.id() == client_id) {
            self.clients.remove(pos);
            true
        } else {
            false
        }
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.clients.len() >= 2
    }

    pub fn is_full_open(&self) -> bool {
        self.is_full() && self.clients.iter().all(|client| client.is_open())
    }

    pub fn should_cleanup(&self) -> bool {
        self.is_empty() || self.clients.iter().all(|c| !c.is_open())
    }

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn clients(&self) -> &[MailboxClient] {
        &self.clients
    }

    pub fn client(&self, client_id: &EitherSide) -> Option<&MailboxClient> {
        self.clients.iter().find(|client| client.id() == client_id)
    }

    pub fn client_mut(&mut self, client_id: &EitherSide) -> Option<&mut MailboxClient> {
        self.clients
            .iter_mut()
            .find(|client| client.id() == client_id)
    }

    pub fn has_client(&self, client_id: &EitherSide) -> bool {
        self.clients.iter().any(|client| client.id() == client_id)
    }

    pub fn open_client(&mut self, client_id: &EitherSide) -> bool {
        for client in &mut self.clients {
            if client.id() == client_id {
                return client.open();
            }
        }

        false
    }

    pub fn release_client(&mut self, client_id: &EitherSide) -> bool {
        for client in &mut self.clients {
            if client_id == client.id() {
                client.release();
                return true;
            }
        }

        false
    }

    pub async fn close_mailbox(&mut self) {
        self.clients.iter_mut().for_each(|client| client.release());
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
