use crate::core::{Mailbox, Nameplate};
use crate::server_messages::*;
use async_std::net::TcpStream;
use async_tungstenite::{tungstenite, WebSocketStream};
use futures::stream::FusedStream;
use futures::{select, FutureExt, SinkExt, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// Limit of max open mailboxes
const MAX_MAILBOXES: usize = 1024;

struct MailboxClient {
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

struct ClaimedMailbox {
    clients: Vec<MailboxClient>,
    channels: Vec<(
        async_channel::Sender<ServerMessage>,
        async_channel::Receiver<ServerMessage>,
    )>,
    creation_time: std::time::Instant,
    last_activity: std::time::Instant,
}

impl ClaimedMailbox {
    pub fn new(first_client_id: &str) -> Self {
        let now = std::time::Instant::now();
        let mut clients = Vec::with_capacity(2);
        clients.push(MailboxClient::new(first_client_id));

        // pre-create the communication channels to store the messages inside
        let channels = vec![async_channel::unbounded(), async_channel::unbounded()];

        Self {
            clients,
            channels,
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

    pub fn client_receiver_of(
        &mut self,
        client_id: &str,
    ) -> Option<async_channel::Receiver<ServerMessage>> {
        if let Some(pos) = self
            .clients
            .iter()
            .position(|client| client.id() == client_id)
        {
            Some(self.channels[pos].1.clone())
        } else {
            None
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

    pub fn close(&mut self) {
        self.clients.iter_mut().for_each(|client| client.close())
    }
}

#[derive(Default)]
struct MailboxServerState {
    mailboxes: HashMap<String, ClaimedMailbox>,
    allocations: HashMap<String, (String, std::time::Instant)>,
}

impl MailboxServerState {
    pub fn try_claim(&mut self, nameplate: &str, client_id: &str) -> Option<String> {
        self.cleanup_allocations();
        self.cleanup_mailboxes();

        if self.mailboxes.len() > MAX_MAILBOXES {
            // Sorry, no mailboxes are free at the moment
            return None;
        }

        if self.mailboxes.get(nameplate).is_none() {
            // We check allocations if the mailbox is not open yet
            if let Some((allocated_client_id, _time)) = self.allocations.get(nameplate) {
                if client_id != allocated_client_id {
                    // This allocation was not for you
                    return None;
                } else {
                    self.allocations.remove(nameplate);
                }
            }

            self.mailboxes
                .insert(nameplate.to_string(), ClaimedMailbox::new(client_id));
            return Some(nameplate.to_string());
        } else if !self.nameplate_has_client(nameplate, client_id) {
            let mailbox = self.mailboxes.get_mut(nameplate);
            if let Some(mailbox) = mailbox {
                if mailbox.add_client(client_id).is_some() {
                    return Some(nameplate.to_string());
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
            if mailbox.is_empty() {
                println!("Mailbox {} removed", nameplate);
                return false;
            }

            // 72 hours after creation in any case
            let creation_duration = Duration::from_secs(60 * 60 * 72);
            if mailbox.creation_time.elapsed() > creation_duration {
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

            if mailbox.last_activity.elapsed() > activity_duration {
                println!("Mailbox {} removed", nameplate);
                false
            } else {
                true
            }
        })
    }

    pub fn allocate(&mut self, client_id: &str) -> Option<String> {
        self.cleanup_allocations();
        if self.allocations.len() + self.mailboxes.len() >= MAX_MAILBOXES {
            // Sorry, we are full at the moment
            return None;
        }

        for key in 1..MAX_MAILBOXES {
            let key = key.to_string();
            if !self.mailboxes.contains_key(&key) && !self.allocations.contains_key(&key) {
                self.allocations.insert(
                    key.clone(),
                    (client_id.to_string(), std::time::Instant::now()),
                );
                return Some(key);
            }
        }

        // MAX_ALLOCATIONS reached
        None
    }

    pub fn nameplate_has_client(&self, nameplate: &str, client_id: &str) -> bool {
        if let Some(mailbox) = self.mailboxes.get(nameplate) {
            mailbox.has_client(client_id)
        } else {
            false
        }
    }

    pub fn remove_client(&mut self, nameplate: &str, client: &str) -> bool {
        println!("Removing client {} from mailbox {}", client, nameplate);
        let mailbox = self.mailboxes.get_mut(nameplate);
        if let Some(mailbox) = mailbox {
            let res = mailbox.remove_client(client);

            if mailbox.is_empty() {
                self.mailboxes.remove(nameplate);
            }

            res
        } else {
            false
        }
    }
}

pub struct MailboxServer {
    listener: async_std::net::TcpListener,
    state: Arc<Mutex<MailboxServerState>>,
}

impl MailboxServer {
    pub async fn new() -> Result<Self, std::io::Error> {
        let addrs: [async_std::net::SocketAddr; 2] =
            ["[::]:0".parse().unwrap(), "0.0.0.0:0".parse().unwrap()];
        let listener = async_std::net::TcpListener::bind(&addrs[..]).await?;
        Ok(Self {
            listener,
            state: Default::default(),
        })
    }

    async fn send_msg(stream: &mut WebSocketStream<TcpStream>, msg: &ServerMessage) {
        let json = serde_json::to_string(msg).unwrap();
        println!("Send: {}", json);
        stream.send(json.into()).await.unwrap();
    }

    async fn protocol_error(mut ws: &mut WebSocketStream<TcpStream>) {
        println!("Failure condition");
        println!("Backtrace: {:?}", backtrace::Backtrace::new());
        Self::send_msg(&mut ws, &ServerMessage::Unknown).await;
    }

    async fn fail_close_mailbox(
        mut ws: &mut WebSocketStream<TcpStream>,
        state: Arc<Mutex<MailboxServerState>>,
        nameplate: &str,
    ) {
        println!("Failure condition");
        println!("Backtrace: {:?}", backtrace::Backtrace::new());
        Self::send_msg(&mut ws, &ServerMessage::Unknown).await;
        state
            .lock()
            .unwrap()
            .mailboxes
            .get_mut(nameplate)
            .map(|m| m.close());
        state.lock().unwrap().mailboxes.remove(nameplate);
    }

    async fn receive_msg(ws: &mut WebSocketStream<TcpStream>) -> Result<ClientMessage, ()> {
        if let Some(msg) = ws.next().await {
            match msg {
                Ok(msg) => {
                    if let tungstenite::Message::Text(msg_txt) = msg {
                        println!("Receive: {}", msg_txt);
                        let client_msg = serde_json::from_str(&msg_txt);
                        if let Ok(client_msg) = client_msg {
                            Ok(client_msg)
                        } else {
                            println!("Error parsing message: {:?}", client_msg);
                            Err(())
                        }
                    } else {
                        println!("Receive unknown message: {}", msg);
                        Err(())
                    }
                }
                Err(err) => {
                    println!("Error: {}", err);
                    Err(())
                }
            }
        } else {
            println!("No message");
            Err(())
        }
    }

    async fn handle_client_msg(
        client_id: &str,
        nameplate: &str,
        state: Arc<Mutex<MailboxServerState>>,
        mut ws: &mut WebSocketStream<TcpStream>,
        client_msg: ClientMessage,
    ) -> Result<(), ()> {
        match client_msg {
            ClientMessage::Add { phase, body } => {
                Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                let mut senders = state
                    .lock()
                    .unwrap()
                    .mailboxes
                    .get_mut(nameplate)
                    .unwrap()
                    .channels
                    .iter_mut()
                    .map(|(sender, _receiver)| sender.clone())
                    .collect::<Vec<async_channel::Sender<ServerMessage>>>();

                for channel in &mut senders {
                    // send the message to the peer channel
                    channel
                        .send(ServerMessage::Message(EncryptedMessage {
                            side: client_id.into(),
                            phase: phase.clone(),
                            body: body.clone(),
                        }))
                        .await
                        .unwrap();
                }

                if senders.len() == 0 {
                    Self::fail_close_mailbox(&mut ws, state.clone(), nameplate).await;
                    Err(())
                } else {
                    Ok(())
                }
            }
            ClientMessage::Release { nameplate } => {
                Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                let released = {
                    let mailboxes = &mut state.lock().unwrap().mailboxes;
                    let mut released = false;
                    if let Some(mailbox) = mailboxes.get_mut(&nameplate) {
                        for client in &mut mailbox.clients {
                            if client_id == client.id() {
                                client.close();
                                released = true;
                            }
                        }
                    }

                    released
                };

                if released {
                    Self::send_msg(&mut ws, &ServerMessage::Released).await;
                    Ok(())
                } else {
                    Self::fail_close_mailbox(&mut ws, state.clone(), &nameplate).await;
                    Err(())
                }
            }
            ClientMessage::Close { mailbox, mood } => {
                Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                println!("Closed mailbox for client: {}. Mood: {}", client_id, mood);
                let closed = {
                    let mailboxes = &mut state.lock().unwrap().mailboxes;
                    if let Some(mailbox) = mailboxes.get_mut(&mailbox.0) {
                        mailbox.remove_client(client_id)
                    } else {
                        false
                    }
                };

                if closed {
                    Self::send_msg(&mut ws, &ServerMessage::Closed).await;
                    Ok(())
                } else {
                    Self::fail_close_mailbox(&mut ws, state.clone(), &nameplate).await;
                    Err(())
                }
            }
            _ => {
                println!("Received unknown message: {}", client_msg);
                Self::fail_close_mailbox(&mut ws, state.clone(), nameplate).await;
                Err(())
            }
        }
    }

    async fn ws_handler(
        state: Arc<Mutex<MailboxServerState>>,
        mut ws: WebSocketStream<TcpStream>,
    ) -> Result<(), ()> {
        #[allow(deprecated)]
        let welcome = ServerMessage::Welcome {
            welcome: WelcomeMessage {
                current_cli_version: None,
                motd: None,
                error: None,
                permission_required: None,
            },
        };
        Self::send_msg(&mut ws, &welcome).await;

        let msg = Self::receive_msg(&mut ws).await?;
        let client_id = match msg {
            ClientMessage::Bind { appid, side } => {
                // TODO: scope by app id
                Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                (**(side)).to_string()
            }
            _ => {
                Self::protocol_error(&mut ws).await;
                return Err(());
            }
        };

        let mut claimed = false;
        let mut client_nameplate = String::new();

        while !claimed {
            // Allocate or claim are the only two messages that can come now
            let msg = Self::receive_msg(&mut ws).await?;
            match &msg {
                ClientMessage::Allocate => {
                    let allocation = state.lock().unwrap().allocate(&client_id);

                    if let Some(allocation) = allocation {
                        Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                        Self::send_msg(
                            &mut ws,
                            &ServerMessage::Allocated {
                                nameplate: Nameplate::new(&allocation),
                            },
                        )
                        .await;
                    } else {
                        Self::protocol_error(&mut ws).await;
                        return Err(());
                    }
                }
                ClientMessage::Claim { nameplate } => {
                    Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                    println!("Claiming nameplate: {}", nameplate);

                    if state
                        .lock()
                        .unwrap()
                        .try_claim(&nameplate, &client_id)
                        .is_none()
                    {
                        Self::send_msg(
                            &mut ws,
                            &ServerMessage::Error {
                                error: "Too many clients connected".to_string(),
                                orig: Box::new(ServerMessage::Unknown),
                            },
                        )
                        .await;
                        return Err(());
                    } else {
                        Self::send_msg(
                            &mut ws,
                            &ServerMessage::Claimed {
                                mailbox: Mailbox(nameplate.to_string()),
                            },
                        )
                        .await;
                        claimed = true;
                        client_nameplate = nameplate.to_string();
                    }
                }
                _ => {
                    Self::protocol_error(&mut ws).await;
                    return Err(());
                }
            };
        }

        // Now we wait for an open message
        let msg = Self::receive_msg(&mut ws).await?;

        match msg {
            ClientMessage::Open { mailbox } => {
                if state
                    .lock()
                    .unwrap()
                    .nameplate_has_client(&mailbox.0, &client_id)
                {
                    let opened = state
                        .lock()
                        .unwrap()
                        .mailboxes
                        .get_mut(&mailbox.0)
                        .unwrap()
                        .open(&client_id);
                    if opened {
                        Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                    } else {
                        Self::fail_close_mailbox(&mut ws, state.clone(), &client_nameplate).await;
                        return Err(());
                    }
                } else {
                    Self::fail_close_mailbox(&mut ws, state.clone(), &client_nameplate).await;
                    return Err(());
                }
            }
            _ => {
                Self::fail_close_mailbox(&mut ws, state.clone(), &client_nameplate).await;
                return Err(());
            }
        }

        let client_receiver = state
            .lock()
            .unwrap()
            .mailboxes
            .get_mut(&client_nameplate)
            .unwrap()
            .client_receiver_of(&client_id)
            .unwrap();

        // Now we are operating on the open channel
        while !ws.is_terminated() {
            let mut client_future = Box::pin(Self::receive_msg(&mut ws)).fuse();
            let mut peer_future = Box::pin(client_receiver.recv()).fuse();

            select! {
                msg = client_future => {
                    drop(client_future);
                    if let Ok(msg) = msg {
                        Self::handle_client_msg(&client_id, &client_nameplate, state.clone(), &mut ws, msg).await.unwrap();
                    } else {
                        Self::protocol_error(&mut ws).await;
                        break;
                    }
                },
                msg = peer_future => {
                    drop(client_future);
                    if let Ok(msg) = msg {
                        println!("Sending msg: {} to peer {}", msg, client_id);
                        Self::send_msg(&mut ws, &msg).await;
                    } else {
                        Self::protocol_error(&mut ws).await;
                        break;
                    }
                }
            }
        }

        println!("Websocket connection terminated");
        let _ = ws.close(None).await;

        Ok(())
    }

    pub async fn wait_for_connection(&mut self) {
        let mut incoming = self.listener.incoming();
        while let Some(stream) = incoming.next().await {
            println!("Connection!");
            if let Ok(stream) = stream {
                let state = self.state.clone();
                async_std::task::spawn(async move {
                    let ws = async_tungstenite::accept_async(stream).await;
                    if let Ok(ws) = ws {
                        Self::ws_handler(state, ws).await.unwrap();
                    }
                });
            }
        }
    }

    pub fn port(&self) -> u16 {
        self.listener.local_addr().unwrap().port()
    }
}
