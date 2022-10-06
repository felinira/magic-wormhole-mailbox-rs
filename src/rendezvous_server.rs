mod mailbox;
mod state;

use self::mailbox::ClaimedMailbox;
use self::state::RendezvousServerState;
use crate::core::{AppID, EitherSide, Mailbox, Nameplate, TheirSide};
use crate::server_messages::*;
use async_std::net::TcpStream;
use async_tungstenite::tungstenite::Error;
use async_tungstenite::{tungstenite, WebSocketStream};
use futures::future::err;
use futures::stream::FusedStream;
use futures::{pending, poll, select, AsyncReadExt, FutureExt, SinkExt, StreamExt};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::task::Poll;
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
    #[error("SendError: {}", source)]
    Send {
        #[from]
        #[source]
        source: tungstenite::Error,
    },
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
                        let mut connection =
                            RendezvousServerConnection::init_connection(state, &mut ws).await;
                        if let Ok(mut connection) = connection {
                            connection.handle_connection().await;
                        }

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

    app: Option<AppID>,
    side: Option<EitherSide>,
    mailbox_id: Option<Mailbox>,
    broadcast_receiver: async_broadcast::Receiver<EncryptedMessage>,
}

impl<'a> RendezvousServerConnection<'a> {
    async fn init_connection(
        state: RendezvousServerState,
        websocket: &'a mut WebSocketStream<TcpStream>,
    ) -> Result<RendezvousServerConnection<'a>, ClientConnectionError> {
        #[allow(deprecated)]
        let welcome = ServerMessage::Welcome {
            welcome: WelcomeMessage {
                current_cli_version: None,
                motd: None,
                error: None,
                permission_required: None,
            },
        };

        Self::send_msg_ws(websocket, &welcome).await?;

        let msg = Self::receive_msg_ws(websocket).await?;
        let (app, side) = match msg {
            ClientMessage::Bind { appid, side } => (appid, EitherSide(side.0.clone())),
            _ => {
                return Err(ClientConnectionError::UnexpectedMessage(msg));
            }
        };

        let mut claimed = false;
        let mut mailbox_id = Mailbox(String::new());

        while !claimed {
            // Allocate or claim are the only two messages that can come now
            let msg = Self::receive_msg_ws(websocket).await?;
            match &msg {
                ClientMessage::Allocate => {
                    let allocation = state.write().app_mut(&app).allocate(&side);

                    if let Some(allocation) = allocation {
                        Self::send_msg_ws(
                            websocket,
                            &ServerMessage::Allocated {
                                nameplate: Nameplate::new(&allocation),
                            },
                        )
                        .await?;
                    } else {
                        return Err(ClientConnectionError::UnexpectedMessage(msg));
                    }
                }
                ClientMessage::Claim { nameplate } => {
                    println!("Claiming nameplate: {}", nameplate);

                    if state
                        .write()
                        .app_mut(&app)
                        .try_claim(&Nameplate(nameplate.to_string()), side.clone())
                        .is_none()
                    {
                        return Err(ClientConnectionError::TooManyClients);
                    } else {
                        Self::send_msg_ws(
                            websocket,
                            &ServerMessage::Claimed {
                                mailbox: Mailbox(nameplate.to_string()),
                            },
                        )
                        .await?;
                        claimed = true;
                        mailbox_id = Mailbox(nameplate.to_string());
                    }
                }
                _ => {
                    return Err(ClientConnectionError::UnexpectedMessage(msg));
                }
            };
        }

        let msg = Self::receive_msg_ws(websocket).await?;
        match &msg {
            ClientMessage::Open { mailbox } => {
                let opened = state
                    .write()
                    .app_mut(&app)
                    .mailboxes_mut()
                    .get_mut(&mailbox)
                    .unwrap()
                    .open_client(&side);
                if !opened {
                    return Err(ClientConnectionError::NotConnectedToMailbox(
                        mailbox.clone(),
                    ));
                }
            }
            _ => {
                return Err(ClientConnectionError::UnexpectedMessage(msg));
            }
        }

        let broadcast_receiver = state
            .write()
            .app_mut(&app)
            .mailbox_mut(&mailbox_id)
            .unwrap()
            .new_broadcast_receiver();

        Ok(Self {
            state,
            websocket,
            app: Some(app),
            side: Some(side),
            mailbox_id: Some(mailbox_id),
            broadcast_receiver,
        })
    }

    pub async fn handle_connection(&mut self) -> Result<(), ClientConnectionError> {
        // Now we are operating on the open channel
        while !self.websocket.is_terminated() {
            select! {
                msg_res = Self::receive_msg_ws(self.websocket).fuse() => {
                    match msg_res {
                        Ok(msg) => {
                            self.handle_client_msg(msg).await.unwrap();
                        }
                        Err(err) => {
                            match err {
                                ReceiveError::Closed(close_frame) => {
                                    // Regular closing of the WebSocket
                                    println!("WebSocket closed by peer: {}", self.side.as_ref().unwrap_or(&EitherSide("???".to_string())));
                                    self.websocket.close(close_frame).await;
                                    break;
                                }
                                err => {
                                    println!("Message receive error: {}", err);
                                    self.handle_error_release(err.into()).await;
                                    break;
                                }
                            }
                        }
                    }
                },
                msg_res = self.broadcast_receiver.recv().fuse() => {
                    match msg_res {
                        Ok(msg) => {
                            println!("Sending msg: {} to peer {}", msg, &self.side.as_ref().unwrap_or(&EitherSide("???".to_string())));
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
        }

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
        Self::send_msg_ws(&mut self.websocket, msg).await
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

        if let (Some(app), Some(mailbox_id), Some(side)) = (&self.app, &self.mailbox_id, &self.side)
        {
            self.state
                .write()
                .app_mut(app)
                .mailbox_mut(mailbox_id)
                .map(|m| m.client_mut(side).map(|c| c.release()));
        }
    }

    #[must_use]
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

                            match client_msg {
                                ClientMessage::Ping { ping } => {
                                    Self::send_msg_ws(
                                        websocket,
                                        &ServerMessage::Pong { pong: ping },
                                    )
                                    .await;
                                    continue;
                                }
                                _ => {}
                            }

                            Ok(client_msg)
                        } else {
                            Err(ReceiveError::JsonParse(msg_txt))
                        }
                    }
                    tungstenite::Message::Close(frame) => Err(ReceiveError::Closed(frame)),
                    msg => Err(ReceiveError::UnexpectedType(msg)),
                },
                Err(err) => Err(err.into()),
            };
        }

        Err(ReceiveError::Closed(None))
    }

    #[must_use]
    async fn receive_msg(&mut self) -> Result<ClientMessage, ReceiveError> {
        Self::receive_msg_ws(self.websocket).await
    }

    #[must_use]
    async fn handle_client_msg(
        &mut self,
        client_msg: ClientMessage,
    ) -> Result<(), ClientConnectionError> {
        match client_msg {
            ClientMessage::Add { phase, body } => {
                if let (Some(app), Some(mailbox_id), Some(side)) =
                    (&self.app, &self.mailbox_id, &self.side)
                {
                    let mut broadcast = self
                        .state
                        .write()
                        .app_mut(&app)
                        .mailbox_mut(&mailbox_id)
                        .unwrap()
                        .broadcast_sender();

                    // send the message to the broadcast channel
                    broadcast
                        .broadcast(EncryptedMessage {
                            side: side.clone().0.into(),
                            phase: phase.clone(),
                            body: body.clone(),
                        })
                        .await
                        .unwrap();

                    // update last activity timestamp
                    self.state
                        .write()
                        .app_mut(&app)
                        .mailbox_mut(&mailbox_id)
                        .unwrap()
                        .update_last_activity();

                    Ok(())
                } else {
                    Err(ClientConnectionError::NotClaimedAnyMailbox)
                }
            }
            ClientMessage::Release { nameplate } => {
                if let (Some(app), Some(mailbox_id), Some(side)) =
                    (&self.app, &self.mailbox_id, &self.side)
                {
                    let released = {
                        let mut released = false;
                        if let Some(mailbox) = self
                            .state
                            .write()
                            .app_mut(app)
                            .mailbox_mut(&Mailbox(nameplate.clone()))
                        {
                            released = mailbox.release_client(&side)
                        }

                        released
                    };

                    if released {
                        self.send_msg(&ServerMessage::Released).await?;
                        Ok(())
                    } else {
                        Err(ClientConnectionError::NotConnectedToMailbox(Mailbox(
                            nameplate,
                        )))
                    }
                } else {
                    Err(ClientConnectionError::NotClaimedAnyMailbox)
                }
            }
            ClientMessage::Close { mailbox, mood } => {
                if let (Some(app), Some(mailbox_id), Some(side)) =
                    (&self.app, &self.mailbox_id, &self.side)
                {
                    if &mailbox != mailbox_id {
                        Err(ClientConnectionError::NotConnectedToMailbox(mailbox))
                    } else {
                        println!("Closed mailbox for client: {}. Mood: {}", side, mood);
                        let closed = {
                            if let Some(mailbox) = self
                                .state
                                .write()
                                .app_mut(app)
                                .mailboxes_mut()
                                .get_mut(&mailbox)
                            {
                                mailbox.remove_client(&side)
                            } else {
                                false
                            }
                        };

                        if closed {
                            self.send_msg(&ServerMessage::Closed).await?;
                            Ok(())
                        } else {
                            Err(ClientConnectionError::NotConnectedToMailbox(mailbox))
                        }
                    }
                } else {
                    Err(ClientConnectionError::NotClaimedAnyMailbox)
                }
            }
            _ => Err(ClientConnectionError::UnexpectedMessage(client_msg)),
        }
    }
}
