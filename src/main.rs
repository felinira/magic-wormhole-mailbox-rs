#![deny(unused_must_use)]
#![allow(unused)]

use crate::zeroconf::{ZeroconfBrowser, ZeroconfService};
use futures::{select, FutureExt};

mod core;
mod network;
mod rendezvous_server;
mod server_messages;
mod zeroconf;

#[async_std::main]
async fn main() {
    let mut mailbox = rendezvous_server::RendezvousServer::new(8080)
        .await
        .unwrap();
    println!("Listening with mailbox port: {}", mailbox.port());

    let service = ZeroconfService::run(mailbox.port());
    let service_receiver = service.runner.receiver.clone();

    let browser = ZeroconfBrowser::run();
    let browser_receiver = browser.runner.receiver.clone();

    loop {
        let mut s = service_receiver.recv().fuse();
        let mut b = browser_receiver.recv().fuse();
        let mut m = Box::pin(mailbox.wait_for_connection()).fuse();

        select! {
            msg = s => println!("{:?}", msg.unwrap()),
            msg = b => println!("{:?}", msg.unwrap()),
            () = m => println!("mailbox msg"),
        }
    }
}
