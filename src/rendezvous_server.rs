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

    #[must_use]
    async fn send_msg(stream: &mut WebSocketStream<TcpStream>, msg: &ServerMessage) {
        let json = serde_json::to_string(msg).unwrap();
        println!("Send: {}", json);
        stream.send(json.into()).await.unwrap();
    }

    async fn handle_error(mut ws: &mut WebSocketStream<TcpStream>, error: ClientConnectionError) {
        println!("An error occurred: {}", error);
        println!("Backtrace: {:?}", backtrace::Backtrace::new());

        if !ws.is_terminated() {
            Self::send_msg(
                &mut ws,
                &ServerMessage::Error {
                    error: error.to_string(),
                    orig: Box::new(ServerMessage::Unknown),
                },
            )
            .await;
        }
    }

    async fn handle_error_release(
        mut ws: &mut WebSocketStream<TcpStream>,
        state: RendezvousServerState,
        mailbox_id: &Mailbox,
        client_id: &EitherSide,
        error: ClientConnectionError,
    ) {
        Self::handle_error(ws, error);

        state
            .write()
            .mailbox_mut(mailbox_id)
            .map(|m| m.client_mut(client_id).map(|c| c.release()));
    }

    async fn receive_msg(
        ws: &mut WebSocketStream<TcpStream>,
    ) -> Result<ClientMessage, ReceiveError> {
        if let Some(msg) = ws.next().await {
            match msg {
                Ok(msg) => match msg {
                    tungstenite::Message::Text(msg_txt) => {
                        println!("Receive: {}", msg_txt);
                        let client_msg = serde_json::from_str(&msg_txt);
                        if let Ok(client_msg) = client_msg {
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
        client_id: &EitherSide,
        mailbox_id: &Mailbox,
        state: RendezvousServerState,
        mut ws: &mut WebSocketStream<TcpStream>,
        client_msg: ClientMessage,
    ) -> Result<(), ClientConnectionError> {
        match client_msg {
            ClientMessage::Add { phase, body } => {
                Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                let mut broadcast = state
                    .write()
                    .mailbox_mut(mailbox_id)
                    .unwrap()
                    .broadcast_sender();

                // send the message to the broadcast channel
                broadcast
                    .broadcast(EncryptedMessage {
                        side: client_id.0.clone().into(),
                        phase: phase.clone(),
                        body: body.clone(),
                    })
                    .await
                    .unwrap();

                // update last activity timestamp
                state
                    .write()
                    .mailbox_mut(mailbox_id)
                    .unwrap()
                    .update_last_activity();

                Ok(())
            }
            ClientMessage::Release { nameplate } => {
                Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                let released = {
                    let mut released = false;
                    if let Some(mailbox) = state
                        .write()
                        .mailboxes_mut()
                        .get_mut(&Mailbox(nameplate.clone()))
                    {
                        released = mailbox.release_client(client_id)
                    }

                    released
                };

                if released {
                    Self::send_msg(&mut ws, &ServerMessage::Released).await;
                    Ok(())
                } else {
                    Err(ClientConnectionError::NotConnectedToMailbox(Mailbox(
                        nameplate,
                    )))
                }
            }
            ClientMessage::Close { mailbox, mood } => {
                Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                println!("Closed mailbox for client: {}. Mood: {}", client_id, mood);
                let closed = {
                    if let Some(mailbox) = state.write().mailboxes_mut().get_mut(&mailbox) {
                        mailbox.remove_client(client_id)
                    } else {
                        false
                    }
                };

                if closed {
                    Self::send_msg(&mut ws, &ServerMessage::Closed).await;
                    Ok(())
                } else {
                    Err(ClientConnectionError::NotConnectedToMailbox(mailbox))
                }
            }
            _ => Err(ClientConnectionError::UnexpectedMessage(client_msg)),
        }
    }

    async fn ws_handler(
        state: RendezvousServerState,
        ws: &mut WebSocketStream<TcpStream>,
    ) -> Result<(), ClientConnectionError> {
        #[allow(deprecated)]
        let welcome = ServerMessage::Welcome {
            welcome: WelcomeMessage {
                current_cli_version: None,
                motd: None,
                error: None,
                permission_required: None,
            },
        };
        Self::send_msg(ws, &welcome).await;

        let msg = Self::receive_msg(ws).await?;
        let client_id: EitherSide = match msg {
            ClientMessage::Bind { appid, side } => {
                // TODO: scope by app id
                Self::send_msg(ws, &ServerMessage::Ack).await;
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
            let msg = Self::receive_msg(ws).await?;
            match &msg {
                ClientMessage::Allocate => {
                    let allocation = state.write().allocate(&client_id);

                    if let Some(allocation) = allocation {
                        Self::send_msg(ws, &ServerMessage::Ack).await;
                        Self::send_msg(
                            ws,
                            &ServerMessage::Allocated {
                                nameplate: Nameplate::new(&allocation),
                            },
                        )
                        .await;
                    } else {
                        return Err(ClientConnectionError::UnexpectedMessage(msg));
                    }
                }
                ClientMessage::Claim { nameplate } => {
                    Self::send_msg(ws, &ServerMessage::Ack).await;
                    println!("Claiming nameplate: {}", nameplate);

                    if state
                        .write()
                        .try_claim(&Nameplate(nameplate.to_string()), client_id.clone())
                        .is_none()
                    {
                        return Err(ClientConnectionError::TooManyClients);
                    } else {
                        Self::send_msg(
                            ws,
                            &ServerMessage::Claimed {
                                mailbox: Mailbox(nameplate.to_string()),
                            },
                        )
                        .await;
                        claimed = true;
                        client_mailbox = Mailbox(nameplate.to_string());
                    }
                }
                _ => {
                    return Err(ClientConnectionError::UnexpectedMessage(msg));
                }
            };
        }

        // Now we wait for an open message
        let msg = Self::receive_msg(ws).await?;

        match msg {
            ClientMessage::Open { mailbox } => {
                let opened = state
                    .write()
                    .mailboxes_mut()
                    .get_mut(&mailbox)
                    .unwrap()
                    .open_client(&client_id);
                if opened {
                    Self::send_msg(ws, &ServerMessage::Ack).await;
                } else {
                    return Err(ClientConnectionError::NotConnectedToMailbox(mailbox));
                }
            }
            _ => {
                return Err(ClientConnectionError::UnexpectedMessage(msg));
            }
        }

        let mut broadcast_receiver = state
            .write()
            .mailboxes_mut()
            .get_mut(&client_mailbox)
            .unwrap()
            .new_broadcast_receiver();

        // Now we are operating on the open channel
        while !ws.is_terminated() {
            let mut client_future = Box::pin(Self::receive_msg(ws)).fuse();
            let mut broadcast_future = Box::pin(broadcast_receiver.recv()).fuse();

            select! {
                msg_res = client_future => {
                    drop(client_future);
                    match msg_res {
                        Ok(msg) => {
                            Self::handle_client_msg(&client_id, &client_mailbox, state.clone(), ws, msg).await.unwrap();
                        },
                        Err(err) => {
                            match err {
                                ReceiveError::Closed(close_frame) => {
                                    // Regular closing of the WebSocket
                                    println!("WebSocket closed by peer: {}", client_id);
                                    ws.close(close_frame);
                                    break;
                                },
                                err => {
                                    println!("Message receive error: {}", err);
                                    Self::handle_error_release(ws, state.clone(), &client_mailbox, &client_id, err.into()).await;
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
                            Self::send_msg(ws, &ServerMessage::Message(msg)).await;
                        },
                        Err(err) => {
                            println!("Message Broadcast error: {}", err);
                            Self::handle_error(ws, ClientConnectionError::Inconsistency).await;
                            break;
                        }
                    }
                }
            }
        }

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
                    if let Ok(mut ws) = ws {
                        Self::ws_handler(state, &mut ws).await.unwrap();

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
