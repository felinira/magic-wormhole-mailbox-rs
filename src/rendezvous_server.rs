mod mailbox;
mod nameplate;
mod state;

use self::state::RendezvousServerState;
use crate::core::{AppID, EitherSide, Mailbox, Nameplate};
use crate::server_messages::*;
use async_std::net::TcpStream;
use async_tungstenite::{tungstenite, WebSocketStream};
use futures::stream::FusedStream;
use futures::{select, FutureExt, SinkExt, StreamExt};
use std::string::ToString;
use std::time::Duration;

static ADVERTISE_VERSION: Option<&'static str> = Some("0.12.0");
static MOTD: Option<&'static str> = Some("Welcome to magic-wormhole.rs");

// How often should mailboxes be cleaned up?
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum ReceiveError {
    #[error("Error parsing JSON message: {:?}", _0)]
    JsonParse(String),
    #[error("Received unexpected message type: {:?}", _0)]
    UnexpectedType(tungstenite::Message),
    #[error("WebSocket error: {}", source)]
    WebSocket {
        #[from]
        #[source]
        source: tungstenite::Error,
    },
    #[error("WebSocket closed")]
    Closed(Option<tungstenite::protocol::CloseFrame<'static>>),
}

#[derive(Debug, thiserror::Error)]
#[must_use]
pub enum ClientConnectionError {
    #[error("ReceiveError: {}", source)]
    Receive {
        #[from]
        #[source]
        source: ReceiveError,
    },
    #[error("SendError: {}", source)]
    Send {
        #[from]
        #[source]
        source: tungstenite::Error,
    },
    #[error("Not bound to any app. Send a bind message first")]
    NotBound,
    #[error("Can only bind once")]
    BindTwice,
    #[error("Can only allocate once")]
    AllocateTwice,
    #[error("Can only claim a mailbox once")]
    ClaimTwice,
    #[error("Nameplate {} was not claimed by this side, can't release", _0)]
    NotClaimed(Nameplate),
    #[error("Received unexpected message: {}", _0)]
    UnexpectedMessage(ClientMessage),
    #[error("Too many clients connected")]
    TooManyClients,
    #[error("Not connected to mailbox {}", _0)]
    NotConnectedToMailbox(Mailbox),
    #[error("Not claimed any mailbox")]
    NotClaimedAnyMailbox,
    #[error("An internal inconsistency has been found")]
    Inconsistency,
}

pub struct RendezvousServer {
    listener: async_std::net::TcpListener,
    state: RendezvousServerState,
    cleanup_task: async_std::task::JoinHandle<()>,
}

impl RendezvousServer {
    pub async fn run(port: u16) -> Result<Self, std::io::Error> {
        let addrs: [async_std::net::SocketAddr; 2] = [
            format!("[::]:{}", port).parse().unwrap(),
            format!("0.0.0.0:{}", port).parse().unwrap(),
        ];

        let listener = async_std::net::TcpListener::bind(&addrs[..]).await?;
        let state = RendezvousServerState::default();

        let state_clone = state.clone();
        let cleanup_task = async_std::task::spawn(async move {
            let mut timer = async_io::Timer::interval(CLEANUP_INTERVAL);
            loop {
                timer.next().await;
                state_clone.write().cleanup_all();
            }
        });

        Ok(Self {
            listener,
            state,
            cleanup_task,
        })
    }

    pub async fn stop(self) {
        // Cancel the regular cleanup timer
        self.cleanup_task.cancel().await;

        // Drops the TCP listener, which will close the connection
    }

    pub async fn wait_for_connection(&mut self) {
        let mut incoming = self.listener.incoming();
        while let Some(Ok(stream)) = incoming.next().await {
            println!("New client connected");

            let state = self.state.clone();
            async_std::task::spawn(async move {
                let Ok(mut ws) = async_tungstenite::accept_async(stream).await else {
                    println!("Connection not a websocket");
                    return;
                };

                let connection = RendezvousServerConnection::bind(state, &mut ws).await;
                if let Ok(mut connection) = connection {
                    if let Err(err) = connection.handle_connection().await {
                        println!("Connection error: {}", err);
                    }
                }

                if !ws.is_terminated() {
                    println!("Closing websocket");
                    match ws.close(None).await {
                        Ok(()) => {}
                        Err(err) => {
                            if !matches!(err, tungstenite::Error::ConnectionClosed) {
                                println!("Websocket connection error: {}", err);
                            }
                        }
                    }
                }
            });
        }
    }

    pub fn port(&self) -> u16 {
        self.listener.local_addr().unwrap().port()
    }
}

pub struct RendezvousServerConnection<'a> {
    state: RendezvousServerState,
    websocket: &'a mut WebSocketStream<TcpStream>,

    app: Option<AppID>,
    side: Option<EitherSide>,
    allocation: Option<Nameplate>,
    mailbox_id: Option<Mailbox>,
    broadcast_receiver: Option<async_broadcast::Receiver<EncryptedMessage>>,
    broadcast_sender: Option<async_broadcast::Sender<EncryptedMessage>>,
}

impl<'a> RendezvousServerConnection<'a> {
    async fn bind(
        state: RendezvousServerState,
        websocket: &'a mut WebSocketStream<TcpStream>,
    ) -> Result<RendezvousServerConnection<'a>, ClientConnectionError> {
        #[allow(deprecated)]
        let welcome = ServerMessage::Welcome {
            welcome: WelcomeMessage {
                current_cli_version: ADVERTISE_VERSION.map(|s| s.to_string()),
                motd: MOTD.map(|s| s.to_string()),
                error: None,
                permission_required: None,
            },
        };

        Self::send_msg_ws(websocket, &welcome).await?;

        Ok(Self {
            state,
            websocket,
            app: None,
            side: None,
            allocation: None,
            mailbox_id: None,
            broadcast_receiver: None,
            broadcast_sender: None,
        })
    }

    async fn handle_receive_err(&mut self, err: ReceiveError) {
        match err {
            ReceiveError::Closed(close_frame) => {
                // Regular closing of the WebSocket
                println!(
                    "WebSocket closed by peer: {}",
                    self.side.as_ref().unwrap_or(&EitherSide("???".to_string()))
                );

                let _ = self.websocket.close(close_frame).await;
            }
            ReceiveError::WebSocket {
                source:
                    tungstenite::Error::Protocol(
                        tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
                    ),
            } => {
                println!(
                    "Connection closed by side {} without closing websocket handshake",
                    self.side.as_ref().unwrap_or(&EitherSide("???".to_string()))
                );

                let _ = self.websocket.close(None).await;
            }
            err => {
                println!("Message receive error: {}", err);
                self.handle_error(err.into()).await;
            }
        }
    }

    pub async fn handle_connection(&mut self) -> Result<(), ClientConnectionError> {
        // Now we are operating on the open channel
        while !self.websocket.is_terminated() {
            if let Some(broadcast_receiver) = &mut self.broadcast_receiver {
                select! {
                    msg_res = Self::receive_msg_ws(self.websocket).fuse() => {
                        match msg_res {
                            Ok(msg) => {
                                if let Err(err) = self.handle_client_msg(msg).await {
                                    println!("error");
                                    self.handle_error(err).await;
                                }
                            }
                            Err(err) => {
                                self.handle_receive_err(err).await;
                                break;}
                        }
                    },
                    msg_res = broadcast_receiver.recv().fuse() => {
                        match msg_res {
                            Ok(msg) => {
                                println!("Sending to {}: msg {}", self.side.as_ref().unwrap(), msg);
                                self.send_msg(&ServerMessage::Message(msg)).await?;
                            }
                            Err(err) => {
                                println!("Message Broadcast error: {}", err);
                                self.handle_error(ClientConnectionError::Inconsistency)
                                    .await;
                                break;
                            }
                        }
                    }
                }
            } else {
                match Self::receive_msg_ws(self.websocket).await {
                    Ok(msg) => {
                        if let Err(err) = self.handle_client_msg(msg).await {
                            println!("error");
                            self.handle_error(err).await;
                        }
                    }
                    Err(err) => {
                        self.handle_receive_err(err).await;
                        break;
                    }
                }
            }
        }

        println!("Websocket closed");
        Ok(())
    }

    async fn send_msg_ws(
        websocket: &mut WebSocketStream<TcpStream>,
        msg: &ServerMessage,
    ) -> Result<(), tungstenite::Error> {
        let json = serde_json::to_string(msg).unwrap();
        println!("Send: {}", json);
        websocket.send(json.into()).await
    }

    async fn send_msg(&mut self, msg: &ServerMessage) -> Result<(), tungstenite::Error> {
        Self::send_msg_ws(self.websocket, msg).await
    }

    async fn handle_error(&mut self, error: ClientConnectionError) {
        println!("An error occurred: {}", error);
        println!("Backtrace: {:?}", backtrace::Backtrace::new());

        if !self.websocket.is_terminated() {
            let _ = self
                .send_msg(&ServerMessage::Error {
                    error: error.to_string(),
                    orig: Box::new(ServerMessage::Unknown),
                })
                .await;
        }

        if let (Some(app), Some(side)) = (&self.app, &self.side) {
            if let Some(nameplate) = &self.allocation {
                // If a nameplate was claimed we release it
                self.state
                    .write()
                    .app_mut(app)
                    .release_nameplate(nameplate, side);
            }

            if let Some(mailbox_id) = &self.mailbox_id {
                // If a mailbox was open we close it
                self.state
                    .write()
                    .app_mut(app)
                    .close_mailbox(mailbox_id, side);
            }
        }
    }

    async fn receive_msg_ws(
        websocket: &mut WebSocketStream<TcpStream>,
    ) -> Result<ClientMessage, ReceiveError> {
        while let Some(msg) = websocket.next().await {
            return match msg {
                Ok(msg) => match msg {
                    tungstenite::Message::Text(msg_txt) => {
                        println!("Receive: {}", msg_txt);
                        let client_msg = serde_json::from_str(&msg_txt);
                        if let Ok(client_msg) = client_msg {
                            Self::send_msg_ws(websocket, &ServerMessage::Ack).await?;
                            Ok(client_msg)
                        } else {
                            Err(ReceiveError::JsonParse(msg_txt))
                        }
                    }
                    tungstenite::Message::Close(frame) => Err(ReceiveError::Closed(frame)),
                    tungstenite::Message::Ping(data) => {
                        websocket.send(tungstenite::Message::Pong(data)).await?;

                        // Wait for a new message, this one isn't interesting
                        continue;
                    }
                    msg => Err(ReceiveError::UnexpectedType(msg)),
                },
                Err(err) => Err(err.into()),
            };
        }

        Err(ReceiveError::Closed(None))
    }

    async fn receive_msg(&mut self) -> Result<ClientMessage, ReceiveError> {
        Self::receive_msg_ws(self.websocket).await
    }

    async fn handle_client_msg(
        &mut self,
        client_msg: ClientMessage,
    ) -> Result<(), ClientConnectionError> {
        match client_msg {
            ClientMessage::Bind { appid, side } => {
                if self.app.is_some() {
                    Err(ClientConnectionError::BindTwice)
                } else {
                    self.app = Some(appid);
                    self.side = Some(EitherSide(side.0.clone()));
                    Ok(())
                }
            }
            ClientMessage::Allocate => {
                let (Some(app), Some(side)) = (&self.app, &self.side) else {
                    return Err(ClientConnectionError::NotBound);
                };

                if self.allocation.is_some() {
                    return Err(ClientConnectionError::AllocateTwice);
                }

                let Some(allocation) = self.state.write().app_mut(app).allocate_nameplate(side) else {
                    return Err(ClientConnectionError::Inconsistency);
                };

                self.send_msg(&ServerMessage::Allocated {
                    nameplate: Nameplate::new(&allocation),
                })
                .await?;

                Ok(())
            }
            ClientMessage::Claim { nameplate } => {
                let (Some(app), Some(side)) = (&self.app, &self.side) else {
                    return Err(ClientConnectionError::NotBound);
                };

                if self.mailbox_id.is_some() {
                    return Err(ClientConnectionError::ClaimTwice);
                }

                println!("Claiming nameplate: {}", nameplate);
                let nameplate = Nameplate(nameplate);

                let Some(mailbox) = self
                    .state
                    .write()
                    .app_mut(app)
                    .claim_nameplate(&nameplate, side) else {
                    return Err(ClientConnectionError::TooManyClients)
                };

                self.mailbox_id = Some(mailbox.clone());
                self.send_msg(&ServerMessage::Claimed {
                    mailbox: mailbox.clone(),
                })
                .await?;
                Ok(())
            }
            ClientMessage::Release { nameplate } => {
                let (Some(app), Some(side)) = (&self.app, &self.side) else {
                    return Err(ClientConnectionError::NotClaimedAnyMailbox);
                };

                let nameplate = Nameplate(nameplate);

                if !self
                    .state
                    .write()
                    .app_mut(app)
                    .release_nameplate(&nameplate, side)
                {
                    return Err(ClientConnectionError::NotClaimed(nameplate));
                }

                println!("Releasing nameplate {}", nameplate.0);
                self.send_msg(&ServerMessage::Released).await?;
                Ok(())
            }
            ClientMessage::Open { mailbox } => {
                let (Some(app), Some(side)) = (&self.app, &self.side) else {
                    return Err(ClientConnectionError::NotClaimedAnyMailbox);
                };

                let mut lock = self.state.write();
                let Some(mailbox) = lock.app_mut(app).open_mailbox(&mailbox, side) else {
                    return Err(ClientConnectionError::NotConnectedToMailbox(
                        mailbox.clone(),
                    ));
                };

                println!("Open mailbox: {}", mailbox.0);
                let claimed_mailbox = lock.app_mut(app).mailboxes_mut().get_mut(&mailbox).unwrap();
                self.mailbox_id = Some(mailbox);

                println!("Broadcast channel created for side {}", side);
                self.broadcast_receiver = Some(claimed_mailbox.new_broadcast_receiver());
                self.broadcast_sender = Some(claimed_mailbox.broadcast_sender());

                Ok(())
            }
            ClientMessage::Add { phase, body } => {
                println!("Received Add!");
                let (Some(app), Some(mailbox_id), Some(side), Some(sender)) = (
                    &self.app,
                    &self.mailbox_id,
                    &self.side,
                    &self.broadcast_sender,
                ) else {
                    return                     Err(ClientConnectionError::NotClaimedAnyMailbox);
                };

                // send the message to the broadcast channel
                sender
                    .broadcast(EncryptedMessage {
                        side: side.clone().0.into(),
                        phase: phase.clone(),
                        body: body.clone(),
                    })
                    .await
                    .unwrap();

                let mut lock = self.state.write();
                let Some(mailbox) = lock.app_mut(app).mailbox_mut(mailbox_id) else {
                    return Err(ClientConnectionError::NotConnectedToMailbox(
                        mailbox_id.clone(),
                    ));
                };

                // update last activity timestamp
                mailbox.update_last_activity();
                Ok(())
            }
            ClientMessage::Close { mailbox, mood } => {
                let (Some(app), Some(mailbox_id), Some(side)) =
                    (&self.app, &self.mailbox_id, &self.side) else {
                    return Err(ClientConnectionError::NotClaimedAnyMailbox)
                };

                if &mailbox != mailbox_id {
                    return Err(ClientConnectionError::NotConnectedToMailbox(mailbox));
                }

                println!("Closed mailbox for client: {}. Mood: {}", side, mood);
                if !self
                    .state
                    .write()
                    .app_mut(app)
                    .close_mailbox(mailbox_id, side)
                {
                    return Err(ClientConnectionError::NotConnectedToMailbox(mailbox));
                }

                self.send_msg(&ServerMessage::Closed).await?;
                Ok(())
            }
            ClientMessage::List => {
                let Some(app) = &self.app else {
                    return Err(ClientConnectionError::NotBound);
                };

                let nameplates = self
                    .state
                    .read()
                    .app(app)
                    .map(|app| app.allocations().keys().cloned().collect())
                    .unwrap_or_default();
                self.send_msg(&ServerMessage::Nameplates { nameplates })
                    .await?;
                Ok(())
            }
            ClientMessage::Ping { ping } => {
                self.send_msg(&ServerMessage::Pong { pong: ping }).await?;
                Ok(())
            }
            ClientMessage::SubmitPermission(_) => {
                Err(ClientConnectionError::UnexpectedMessage(client_msg))
            }
        }
    }
}
