#![deny(unused_must_use)]
#![feature(slice_index_methods)]
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio;
use tokio::sync::Mutex;
use std::collections::BTreeMap;
use std::sync::{Arc};
use crate::support::*;
use crate::nbd::handle_packet;

mod nbd;
mod support;
mod utils;
const CLOUDDRIVE_ADDR: &str = "127.0.0.1:19191";
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("CloudDrive Started!");
    let mut listener = TcpListener::bind(&CLOUDDRIVE_ADDR).await?;

    let providers={
        let mut providers: BTreeMap<String, Arc<Mutex<Box<dyn CloudProvider>>>>=BTreeMap::new();
        providers.insert(String::from("memory"), Arc::new(Mutex::new(Box::new(ByteGranularityProvider::new(LRUProvider::new(MemoryProvider::new(1*1024*1024*1024), 1024))))));
        providers.insert(String::from("seafile"), Arc::new(Mutex::new(Box::new(
            ByteGranularityProvider::new(
                LRUProvider::new(
                    SeafileProvider::connect(&std::env::var("SEAFILE_TOKEN").expect("SEAFILE_TOKEN missing!"), &std::env::var("SEAFILE_LIBRARY").expect("SEAFILE_LIBRARY missing!"), 1*1024*1024*1024).await?
                    ,1024*1024
                )
            )
        ))));

        Arc::new(providers)
    };
    println!("CloudDrive Started!");
    loop {
        let (mut socket, _) = listener.accept().await?;
        let ref_providers=Arc::clone(&providers);
        tokio::spawn(async move {
                let provider = nbd::handshake(&mut socket, ref_providers.as_ref()).await.unwrap();
                let mut lock=provider.lock().await;
                handle_packet(&mut socket, &mut *lock).await.unwrap();
        });
    }
}