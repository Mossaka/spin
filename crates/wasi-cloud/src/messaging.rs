use crate::{
    wasi_messaging::wasi::messaging::{
        messaging_types::FormatSpec,
        producer::{self, Channel, Message},
    },
    WasiCloud,
};
use anyhow::{anyhow, bail, Result};
use redis::{aio::Connection, AsyncCommands};
use spin_core::async_trait;
use std::collections::hash_map::Entry;

pub type Error = anyhow::Error;

pub struct Client {
    address: String,
}

#[async_trait]
impl producer::Host for WasiCloud {
    async fn send(
        &mut self,
        client: producer::Client,
        channel: Channel,
        messages: Vec<Message>,
    ) -> Result<Result<(), producer::Error>> {
        let address = self
            .messaging
            .clients
            .get(client)
            .ok_or_else(|| anyhow!("unknown handle: {client}"))?
            .address
            .clone();

        let result = async {
            let conn = self.get_conn(&address).await?;
            for message in messages {
                if message.format != FormatSpec::Raw {
                    bail!("format {:?} not yet supported", message.format);
                }
                if message.metadata.is_some() {
                    bail!("message metadata not yet supported");
                }
                conn.publish(&channel, &message.data).await?;
            }
            Ok(())
        }
        .await;

        Ok(match result {
            Ok(()) => Ok(()),
            Err(e) => Err(self
                .messaging
                .errors
                .push(e)
                .map_err(|()| anyhow!("table overflow"))?),
        })
    }
}

impl WasiCloud {
    async fn get_conn(&mut self, address: &str) -> Result<&mut Connection> {
        let conn = match self.messaging.connections.entry(address.to_string()) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let conn = redis::Client::open(address)?.get_async_connection().await?;
                v.insert(conn)
            }
        };
        Ok(conn)
    }
}
