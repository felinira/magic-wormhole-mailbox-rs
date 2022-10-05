use crate::server_messages::{EncryptedMessage, ServerMessage};

pub type MessageChannelSender = async_channel::Sender<ServerMessage>;
pub type MessageChannelReceiver = async_channel::Receiver<ServerMessage>;

pub(crate) struct MailboxClient {
    id: String,
    is_open: bool,
    was_closed: bool,
}

impl MailboxClient {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            is_open: false,
            was_closed: false,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// Can only open once
    pub fn open(&mut self) -> bool {
        if self.is_open {
            return false;
        }

        if !self.was_closed {
            self.is_open = true;
        }

        self.is_open
    }

    pub fn close(&mut self) {
        self.is_open = false;
        self.was_closed = true;
    }

    pub fn is_open(&self) -> bool {
        self.is_open
    }
}

pub(crate) struct ClaimedMailbox {
    clients: Vec<MailboxClient>,
    broadcast_sender: MessageChannelSender,
    broadcast_receiver: MessageChannelReceiver,
    message_history: Vec<EncryptedMessage>,
    creation_time: std::time::Instant,
    last_activity: std::time::Instant,
}

impl ClaimedMailbox {
    pub fn new(first_client_id: &str) -> Self {
        let now = std::time::Instant::now();
        let mut clients = Vec::with_capacity(2);
        clients.push(MailboxClient::new(first_client_id));

        let (broadcast_sender, broadcast_receiver) = async_channel::unbounded();

        Self {
            clients,
            broadcast_sender,
            broadcast_receiver,
            message_history: Vec::new(),
            creation_time: now,
            last_activity: now,
        }
    }

    pub fn add_client(&mut self, client_id: &str) -> Option<&MailboxClient> {
        if self.is_full() {
            None
        } else {
            let client = MailboxClient::new(client_id);
            self.clients.push(client);
            self.clients.get(self.clients.len() - 1)
        }
    }

    pub fn remove_client(&mut self, client_id: &str) -> bool {
        if let Some(pos) = self.clients.iter().position(|c| c.id == client_id) {
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

    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn clients(&self) -> &[MailboxClient] {
        &self.clients
    }

    pub fn has_client(&self, client_id: &str) -> bool {
        self.clients
            .iter()
            .find(|client| client.id() == client_id)
            .is_some()
    }

    pub fn open(&mut self, client_id: &str) -> bool {
        for client in &mut self.clients {
            if client.id() == client_id {
                return client.open();
            }
        }

        false
    }

    pub fn close_all(&mut self) {
        self.clients.iter_mut().for_each(|client| client.close())
    }

    pub fn close_client(&mut self, client_id: &str) -> bool {
        for client in &mut self.clients {
            if client_id == client.id() {
                client.close();
                return true;
            }
        }

        return false;
    }

    pub fn broadcast_receiver(&self) -> MessageChannelReceiver {
        self.broadcast_receiver.clone()
    }

    pub fn broadcast_sender(&self) -> MessageChannelSender {
        self.broadcast_sender.clone()
    }

    pub fn creation_time(&self) -> &std::time::Instant {
        &self.creation_time
    }

    pub fn last_activity(&self) -> &std::time::Instant {
        &self.last_activity
    }
}
