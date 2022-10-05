mod mailbox;
mod state;

use self::mailbox::ClaimedMailbox;
use self::state::RendezvousServerState;
use crate::core::{EitherSide, Mailbox, Nameplate, TheirSide};
use crate::server_messages::*;
use async_std::net::TcpStream;
use async_tungstenite::tungstenite::Error;
use async_tungstenite::{tungstenite, WebSocketStream};
use futures::future::err;
use futures::stream::FusedStream;
use futures::{select, AsyncReadExt, FutureExt, SinkExt, StreamExt};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

// How often should mailboxes be cleaned up?
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
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
pub enum ClientConnectionError {
    #[error("ReceiveError: {}", source)]
    Receive {
        #[from]
        #[source]
        source: ReceiveError,
    },
    #[error("Received unexpected message: {}", _0)]
    UnexpectedMessage(ClientMessage),
    #[error("Too many clients connected")]
    TooManyClients,
    #[error("Not connected to mailbox {}", _0)]
    NotConnectedToMailbox(Mailbox),
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
                state_clone.write().cleanup_allocations();
                state_clone.write().cleanup_mailboxes();
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
        self.cleanup_task.cancel();

        // Drops the TCP listener, which will close the connection
    }

    pub async fn wait_for_connection(&mut self) {
        let mut incoming = self.listener.incoming();
        while let Some(stream) = incoming.next().await {
            println!("Connection!");
            if let Ok(stream) = stream {
                let state = self.state.clone();
                async_std::task::spawn(async move {
                    let ws = async_tungstenite::accept_async(stream).await;
                    if let Ok(mut ws) = ws {
                        let mut connection = RendezvousServerConnection::new(state, &mut ws);
                        connection.handle_connection().await;

                        if !ws.is_terminated() {
                            println!("Closing websocket");
                            let mut res = ws.close(None).await;

                            match res {
                                Ok(()) => {}
                                Err(err) => {
                                    if !matches!(tungstenite::Error::ConnectionClosed, err) {
                                        println!("Websocket connection error: {}", err);
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }
    }

    pub fn port(&self) -> u16 {
        self.listener.local_addr().unwrap().port()
    }
}

pub struct RendezvousServerConnection<'a> {
    state: RendezvousServerState,
    websocket: &'a mut WebSocketStream<TcpStream>,

    app: Option<String>,
    side: Option<EitherSide>,

    did_allocate: bool,
    did_claim: bool,
    nameplate_id: Option<Nameplate>,
    mailbox_id: Option<Mailbox>,

    did_release: bool,
    did_open: bool,
}

impl<'a> RendezvousServerConnection<'a> {
    fn new(state: RendezvousServerState, websocket: &'a mut WebSocketStream<TcpStream>) -> Self {
        Self {
            state,
            websocket,
            app: None,
            side: None,
            did_allocate: false,
            did_claim: false,
            nameplate_id: None,
            mailbox_id: None,
            did_release: false,
            did_open: false,
        }
    }

    async fn handle_connection(&mut self) -> Result<(), ClientConnectionError> {
        #[allow(deprecated)]
        let welcome = ServerMessage::Welcome {
            welcome: WelcomeMessage {
                current_cli_version: None,
                motd: None,
                error: None,
                permission_required: None,
            },
        };

        self.send_msg(&welcome).await;

        let msg = self.receive_msg().await?;
        let client_id: EitherSide = match msg {
            ClientMessage::Bind { appid, side } => {
                self.side = Some(EitherSide(side.0.clone()));
                self.app = Some(appid.to_string());

                // TODO: scope by app id
                EitherSide(side.0.clone())
            }
            _ => {
                return Err(ClientConnectionError::UnexpectedMessage(msg));
            }
        };

        let mut claimed = false;
        let mut client_mailbox = Mailbox(String::new());

        while !claimed {
            // Allocate or claim are the only two messages that can come now
            let msg = self.receive_msg().await?;
            match &msg {
                ClientMessage::Allocate => {
                    let allocation = self.state.write().allocate(&client_id);

                    if let Some(allocation) = allocation {
                        self.send_msg(&ServerMessage::Allocated {
                            nameplate: Nameplate::new(&allocation),
                        })
                        .await;
                    } else {
                        return Err(ClientConnectionError::UnexpectedMessage(msg));
                    }
                }
                ClientMessage::Claim { nameplate } => {
                    println!("Claiming nameplate: {}", nameplate);

                    if self
                        .state
                        .write()
                        .try_claim(&Nameplate(nameplate.to_string()), client_id.clone())
                        .is_none()
                    {
                        return Err(ClientConnectionError::TooManyClients);
                    } else {
                        self.send_msg(&ServerMessage::Claimed {
                            mailbox: Mailbox(nameplate.to_string()),
                        })
                        .await;
                        claimed = true;
                        client_mailbox = Mailbox(nameplate.to_string());
                        self.mailbox_id = Some(client_mailbox);
                    }
                }
                _ => {
                    return Err(ClientConnectionError::UnexpectedMessage(msg));
                }
            };
        }

        // Now we wait for an open message
        let msg = self.receive_msg().await?;

        match msg {
            ClientMessage::Open { mailbox } => {
                let opened = self
                    .state
                    .write()
                    .mailboxes_mut()
                    .get_mut(&mailbox)
                    .unwrap()
                    .open_client(&client_id);
                if !opened {
                    return Err(ClientConnectionError::NotConnectedToMailbox(mailbox));
                }
            }
            _ => {
                return Err(ClientConnectionError::UnexpectedMessage(msg));
            }
        }

        let mut broadcast_receiver = self
            .state
            .write()
            .mailbox_mut(self.mailbox_id.as_ref().unwrap())
            .unwrap()
            .new_broadcast_receiver();

        // Now we are operating on the open channel
        while !self.websocket.is_terminated() {
            let mut client_future = Box::pin(self.receive_msg()).fuse();
            let mut broadcast_future = Box::pin(broadcast_receiver.recv()).fuse();

            select! {
                msg_res = client_future => {
                    drop(client_future);
                    match msg_res {
                        Ok(msg) => {
                            self.handle_client_msg(msg).await.unwrap();
                        },
                        Err(err) => {
                            match err {
                                ReceiveError::Closed(close_frame) => {
                                    // Regular closing of the WebSocket
                                    println!("WebSocket closed by peer: {}", client_id);
                                    self.websocket.close(close_frame);
                                    break;
                                },
                                err => {
                                    println!("Message receive error: {}", err);
                                    self.handle_error_release(err.into()).await;
                                    break;
                                }
                            }
                        }
                    }
                },
                msg_res = broadcast_future => {
                    drop(client_future);
                    match msg_res {
                        Ok(msg) => {
                            println!("Sending msg: {} to peer {}", msg, client_id);
                            self.send_msg(&ServerMessage::Message(msg)).await;
                        },
                        Err(err) => {
                            println!("Message Broadcast error: {}", err);
                            self.handle_error(ClientConnectionError::Inconsistency).await;
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_msg(&mut self, msg: &ServerMessage) {
        let json = serde_json::to_string(msg).unwrap();
        println!("Send: {}", json);
        self.websocket.send(json.into()).await.unwrap();
    }

    async fn handle_error(&mut self, error: ClientConnectionError) {
        println!("An error occurred: {}", error);
        println!("Backtrace: {:?}", backtrace::Backtrace::new());

        if !self.websocket.is_terminated() {
            self.send_msg(&ServerMessage::Error {
                error: error.to_string(),
                orig: Box::new(ServerMessage::Unknown),
            })
            .await;
        }
    }

    async fn handle_error_release(&mut self, error: ClientConnectionError) {
        self.handle_error(error);

        if let Some(mailbox_id) = &self.mailbox_id {
            if let Some(client_id) = &self.side {
                self.state
                    .write()
                    .mailbox_mut(mailbox_id)
                    .map(|m| m.client_mut(client_id).map(|c| c.release()));
            }
        }
    }

    async fn receive_msg(&mut self) -> Result<ClientMessage, ReceiveError> {
        if let Some(msg) = self.websocket.next().await {
            match msg {
                Ok(msg) => match msg {
                    tungstenite::Message::Text(msg_txt) => {
                        println!("Receive: {}", msg_txt);
                        let client_msg = serde_json::from_str(&msg_txt);
                        if let Ok(client_msg) = client_msg {
                            self.send_msg(&ServerMessage::Ack).await;
                            Ok(client_msg)
                        } else {
                            Err(ReceiveError::JsonParse(msg_txt))
                        }
                    }
                    tungstenite::Message::Close(frame) => Err(ReceiveError::Closed(frame)),
                    msg => Err(ReceiveError::UnexpectedType(msg)),
                },
                Err(err) => Err(err.into()),
            }
        } else {
            Err(ReceiveError::Closed(None))
        }
    }

    #[must_use]
    async fn handle_client_msg(
        &mut self,
        client_msg: ClientMessage,
    ) -> Result<(), ClientConnectionError> {
        match client_msg {
            ClientMessage::Add { phase, body } => {
                if let Some(mailbox_id) = &self.mailbox_id {
                    let mut broadcast = self
                        .state
                        .write()
                        .mailbox_mut(mailbox_id)
                        .unwrap()
                        .broadcast_sender();

                    // send the message to the broadcast channel
                    broadcast
                        .broadcast(EncryptedMessage {
                            side: self.side.as_ref().unwrap().clone().0.into(),
                            phase: phase.clone(),
                            body: body.clone(),
                        })
                        .await
                        .unwrap();

                    // update last activity timestamp
                    self.state
                        .write()
                        .mailbox_mut(mailbox_id)
                        .unwrap()
                        .update_last_activity();

                    Ok(())
                } else {
                    Err(ClientConnectionError::Inconsistency)
                }
            }
            ClientMessage::Release { nameplate } => {
                let released = {
                    let mut released = false;
                    if let Some(mailbox) = self
                        .state
                        .write()
                        .mailboxes_mut()
                        .get_mut(&Mailbox(nameplate.clone()))
                    {
                        released = mailbox.release_client(self.side.as_ref().unwrap())
                    }

                    released
                };

                if released {
                    self.send_msg(&ServerMessage::Released).await;
                    Ok(())
                } else {
                    Err(ClientConnectionError::NotConnectedToMailbox(Mailbox(
                        nameplate,
                    )))
                }
            }
            ClientMessage::Close { mailbox, mood } => {
                println!(
                    "Closed mailbox for client: {}. Mood: {}",
                    self.side.as_ref().unwrap(),
                    mood
                );
                let closed = {
                    if let Some(mailbox) = self.state.write().mailboxes_mut().get_mut(&mailbox) {
                        mailbox.remove_client(&self.side.as_ref().unwrap())
                    } else {
                        false
                    }
                };

                if closed {
                    self.send_msg(&ServerMessage::Closed).await;
                    Ok(())
                } else {
                    Err(ClientConnectionError::NotConnectedToMailbox(mailbox))
                }
            }
            _ => Err(ClientConnectionError::UnexpectedMessage(client_msg)),
        }
    }
}
