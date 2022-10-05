mod mailbox;
mod state;

use self::mailbox::ClaimedMailbox;
use self::state::RendezvousServerState;
use crate::core::{Mailbox, Nameplate};
use crate::server_messages::*;
use async_std::net::TcpStream;
use async_tungstenite::{tungstenite, WebSocketStream};
use futures::stream::FusedStream;
use futures::{select, FutureExt, SinkExt, StreamExt};
use std::collections::HashMap;
use std::time::Duration;

pub struct RendezvousServer {
    listener: async_std::net::TcpListener,
    state: RendezvousServerState,
}

impl RendezvousServer {
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
        state: RendezvousServerState,
        nameplate: &str,
    ) {
        println!("Failure condition");
        println!("Backtrace: {:?}", backtrace::Backtrace::new());
        Self::send_msg(&mut ws, &ServerMessage::Unknown).await;
        state
            .write()
            .unwrap()
            .mailboxes_mut()
            .get_mut(nameplate)
            .map(|m| m.close_all());
        state.write().unwrap().mailboxes_mut().remove(nameplate);
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
        state: RendezvousServerState,
        mut ws: &mut WebSocketStream<TcpStream>,
        client_msg: ClientMessage,
    ) -> Result<(), ()> {
        match client_msg {
            ClientMessage::Add { phase, body } => {
                Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                let mut broadcast = state
                    .write()
                    .unwrap()
                    .mailboxes_mut()
                    .get_mut(nameplate)
                    .unwrap()
                    .broadcast_sender();

                // send the message to the broadcast channel
                broadcast
                    .send(ServerMessage::Message(EncryptedMessage {
                        side: client_id.into(),
                        phase: phase.clone(),
                        body: body.clone(),
                    }))
                    .await
                    .unwrap();

                Ok(())
            }
            ClientMessage::Release { nameplate } => {
                Self::send_msg(&mut ws, &ServerMessage::Ack).await;
                let released = {
                    let mut released = false;
                    if let Some(mailbox) =
                        state.write().unwrap().mailboxes_mut().get_mut(&nameplate)
                    {
                        released = mailbox.close_client(client_id)
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
                    if let Some(mailbox) =
                        state.write().unwrap().mailboxes_mut().get_mut(&mailbox.0)
                    {
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
        state: RendezvousServerState,
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
                    let allocation = state.write().unwrap().allocate(&client_id);

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
                        .write()
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
                    .read()
                    .unwrap()
                    .nameplate_has_client(&mailbox.0, &client_id)
                {
                    let opened = state
                        .write()
                        .unwrap()
                        .mailboxes_mut()
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

        let broadcast_receiver = state
            .write()
            .unwrap()
            .mailboxes_mut()
            .get_mut(&client_nameplate)
            .unwrap()
            .broadcast_receiver();

        // Now we are operating on the open channel
        while !ws.is_terminated() {
            let mut client_future = Box::pin(Self::receive_msg(&mut ws)).fuse();
            let mut broadcast_future = Box::pin(broadcast_receiver.recv()).fuse();

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
                msg = broadcast_future => {
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
