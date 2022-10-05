use crate::network;
use once_cell::sync::Lazy;
use std::any::Any;
use std::sync::Arc;
use std::thread::Thread;
use std::time;
use std::time::Duration;
use zeroconf::prelude::*;
use zeroconf::{
    MdnsBrowser, MdnsService, ServiceDiscovery, ServiceRegistration, ServiceType, TxtRecord,
};

static SERVICE_UUID: Lazy<uuid::Uuid> = Lazy::new(uuid::Uuid::new_v4);

#[derive(Debug)]
pub(crate) enum ZeroconfEvent {
    Error(zeroconf::error::Error),
    ServiceRegistered,
    ServiceDiscovered(ServiceDiscovery),
}

pub(crate) struct ZeroconfRunner {
    pub thread: std::thread::JoinHandle<Thread>,
    pub receiver: async_channel::Receiver<ZeroconfEvent>,
}

pub(crate) struct ZeroconfService {
    pub runner: ZeroconfRunner,
}

impl ZeroconfService {
    pub fn run(mailbox_port: u16) -> Self {
        println!("Running service");

        let (sender, receiver) = async_channel::unbounded();

        let thread = std::thread::spawn(move || {
            let port = network::get_random_port();
            let mut service =
                MdnsService::new(ServiceType::new("magic-wormhole", "tcp").unwrap(), port);
            let mut txt_record = TxtRecord::new();

            txt_record
                .insert("uuid", &SERVICE_UUID.to_string())
                .unwrap();
            txt_record
                .insert("mailbox-port", &mailbox_port.to_string())
                .unwrap();

            service.set_registered_callback(Box::new(Self::on_service_registered));
            service.set_context(Box::new(sender));
            service.set_txt_record(txt_record);

            let event_loop = service.register().unwrap();

            loop {
                std::thread::sleep(time::Duration::from_secs(1));
                event_loop.poll(Duration::from_secs(0)).unwrap();
            }
        });

        Self {
            runner: ZeroconfRunner { thread, receiver },
        }
    }

    fn on_service_registered(
        result: zeroconf::Result<ServiceRegistration>,
        context: Option<Arc<dyn Any>>,
    ) {
        let context = context.unwrap();
        let sender = context
            .downcast_ref::<async_channel::Sender<ZeroconfEvent>>()
            .unwrap();

        match result {
            Ok(service) => {
                println!("Service registered: {:?}", service);
                sender
                    .send_blocking(ZeroconfEvent::ServiceRegistered)
                    .unwrap();
            }
            Err(error) => {
                println!("Error registering service: {}", error);
                sender.send_blocking(ZeroconfEvent::Error(error)).unwrap();
            }
        }
    }

    pub fn stop(&mut self) {}
}

pub(crate) struct ZeroconfBrowser {
    pub runner: ZeroconfRunner,
}

impl ZeroconfBrowser {
    pub fn run() -> Self {
        println!("Running listener");

        let (sender, receiver) = async_channel::unbounded();

        let thread = std::thread::spawn(|| {
            let mut browser = MdnsBrowser::new(ServiceType::new("magic-wormhole", "tcp").unwrap());
            browser.set_service_discovered_callback(Box::new(Self::on_service_discovered));
            browser.set_context(Box::new(sender));
            let event_loop = browser.browse_services().unwrap();

            loop {
                std::thread::sleep(time::Duration::from_secs(1));
                event_loop.poll(Duration::from_secs(0)).unwrap();
            }
        });

        Self {
            runner: ZeroconfRunner { thread, receiver },
        }
    }

    fn on_service_discovered(
        result: zeroconf::Result<ServiceDiscovery>,
        context: Option<Arc<dyn Any>>,
    ) {
        let context = context.unwrap();
        let sender = context
            .downcast_ref::<async_channel::Sender<ZeroconfEvent>>()
            .unwrap();

        match result {
            Ok(service) => {
                if service.service_type().name() != "magic-wormhole" {
                    return;
                }

                let txt = match service.txt() {
                    None => return,
                    Some(txt) => txt,
                };

                if !txt.contains_key("uuid") {
                    return;
                }

                if txt.get("uuid") == Some(SERVICE_UUID.to_string()) {
                    println!("Discovered myself");
                    return;
                }

                println!("Service discovered: {:?}", service);
                sender
                    .send_blocking(ZeroconfEvent::ServiceDiscovered(service))
                    .unwrap();
            }
            Err(err) => {
                println!("Error: {}", err);
            }
        }
    }
}
