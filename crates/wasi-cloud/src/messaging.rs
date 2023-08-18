use crate::{
    wasi_messaging::wasi::messaging::{
        consumer,
        messaging_types::{self, Channel, FormatSpec, GuestConfiguration, Message},
        producer,
    },
    WasiCloud,
};
use anyhow::{anyhow, bail, Result};
use redis::{aio::Connection, AsyncCommands};
use spin_core::async_trait;
use std::{collections::hash_map::Entry, sync::Arc};
use tokio::sync::Mutex as AsyncMutex;

pub type Error = anyhow::Error;

#[derive(Clone)]
pub struct Client(Arc<AsyncMutex<Connection>>);

#[async_trait]
impl producer::Host for WasiCloud {
    async fn send(
        &mut self,
        client: messaging_types::Client,
        channel: Channel,
        messages: Vec<Message>,
    ) -> Result<Result<(), messaging_types::Error>> {
        let client = self
            .messaging
            .clients
            .get(client)
            .ok_or_else(|| anyhow!("unknown handle: {client}"))?
            .clone();

        let result = async {
            for message in messages {
                if message.format != FormatSpec::Raw {
                    bail!("format {:?} not yet supported", message.format);
                }
                if message.metadata.is_some() {
                    bail!("message metadata not yet supported");
                }
                client
                    .0
                    .lock()
                    .await
                    .publish(&channel, &message.data)
                    .await?;
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
    async fn get_client(&mut self, address: &str) -> Result<Client> {
        // TODO: limit cache size and/or time-to-live (e.g. by moving entries which are no longer in use to an
        // `lru::LruCache`)
        Ok(
            match self.messaging.client_cache.entry(address.to_string()) {
                Entry::Occupied(o) => o.get().clone(),
                Entry::Vacant(v) => {
                    let client = Client(Arc::new(AsyncMutex::new(
                        redis::Client::open(address)?.get_async_connection().await?,
                    )));
                    v.insert(client.clone());
                    client
                }
            },
        )
    }
}

#[async_trait]
impl messaging_types::Host for WasiCloud {
    async fn connect(
        &mut self,
        address: String,
    ) -> Result<Result<messaging_types::Client, messaging_types::Error>> {
        let client = self.get_client(&address).await;

        Ok(match client {
            Ok(client) => Ok(self
                .messaging
                .clients
                .push(client)
                .map_err(|()| anyhow!("table overflow"))?),
            Err(e) => Err(self
                .messaging
                .errors
                .push(e)
                .map_err(|()| anyhow!("table overflow"))?),
        })
    }

    async fn disconnect(&mut self, client: messaging_types::Client) -> Result<()> {
        self.messaging
            .clients
            .remove(client)
            .map(drop)
            .ok_or_else(|| anyhow!("unknown handle: {client}"))
    }

    async fn trace(&mut self, error: messaging_types::Error) -> Result<String> {
        Ok(format!(
            "{:?}",
            self.messaging
                .errors
                .get(error)
                .ok_or_else(|| anyhow!("unknown handle: {error}"))?
        ))
    }

    async fn drop_error(&mut self, error: messaging_types::Error) -> Result<()> {
        self.messaging
            .errors
            .get(error)
            .map(drop)
            .ok_or_else(|| anyhow!("unknown handle: {error}"))
    }
}

#[async_trait]
impl consumer::Host for WasiCloud {
    async fn subscribe_try_receive(
        &mut self,
        client: messaging_types::Client,
        channel: Channel,
        timeout_milliseconds: u32,
    ) -> Result<Result<Option<Vec<Message>>, messaging_types::Error>> {
        _ = (client, channel, timeout_milliseconds);
        bail!("todo")
    }

    async fn subscribe_receive(
        &mut self,
        client: messaging_types::Client,
        channel: Channel,
    ) -> Result<Result<Vec<Message>, messaging_types::Error>> {
        _ = (client, channel);
        bail!("todo")
    }

    async fn update_guest_configuration(
        &mut self,
        config: GuestConfiguration,
    ) -> Result<Result<(), messaging_types::Error>> {
        _ = config;
        bail!("todo")
    }

    async fn complete_message(
        &mut self,
        message: Message,
    ) -> Result<Result<(), messaging_types::Error>> {
        _ = message;
        bail!("todo")
    }

    async fn abandon_message(
        &mut self,
        message: Message,
    ) -> Result<Result<(), messaging_types::Error>> {
        _ = message;
        bail!("todo")
    }
}
