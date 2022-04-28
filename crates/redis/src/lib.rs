//! Implementation for the Spin Redis engine.

mod spin;

use crate::spin::SpinRedisExecutor;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;
use redis::{Client, ConnectionLike};
use spin_engine::Builder;
use spin_manifest::{
    Application, ComponentMap, CoreComponent, RedisConfig, RedisTriggerConfiguration,
};
use spin_redis::SpinRedisData;
use spin_trigger::Trigger;
use std::{collections::HashMap, sync::Arc};

wit_bindgen_wasmtime::import!("../../wit/ephemeral/spin-redis.wit");

type ExecutionContext = spin_engine::ExecutionContext<SpinRedisData>;
type RuntimeContext = spin_engine::RuntimeContext<SpinRedisData>;

/// The Spin Redis trigger.
#[derive(Clone)]
pub struct RedisTrigger {
    /// Trigger configuration.
    trigger_config: RedisTriggerConfiguration,
    /// Component trigger configurations.
    component_triggers: ComponentMap<RedisConfig>,
    /// Spin execution context.
    engine: Arc<ExecutionContext>,
    /// Map from channel name to tuple of component name & index.
    subscriptions: HashMap<String, usize>,
}

#[async_trait]
impl Trigger for RedisTrigger {
    /// Run the Redis trigger indefinitely.
    async fn run(&self) -> Result<()> {
        let address = self.trigger_config.address.as_str();

        log::info!("Connecting to Redis server at {}", address);
        let mut client = Client::open(address.to_string())?;
        let mut pubsub = client.get_async_connection().await?.into_pubsub();

        // Subscribe to channels
        for (subscription, idx) in self.subscriptions.iter() {
            let name = &self.engine.config.components[*idx].id;
            log::info!(
                "Subscribed component #{} ({}) to channel: {}",
                idx,
                name,
                subscription
            );
            pubsub.subscribe(subscription).await?;
        }

        let mut stream = pubsub.on_message();
        loop {
            match stream.next().await {
                Some(msg) => drop(self.handle(msg).await),
                None => {
                    log::trace!("Empty message");
                    if !client.check_connection() {
                        log::info!("No Redis connection available");
                        break Ok(());
                    }
                }
            };
        }
    }
}

impl RedisTrigger {
    /// Create a new Spin Redis trigger.
    pub async fn new(
        builder: Builder<SpinRedisData>,
        app: Application<CoreComponent>,
    ) -> Result<Self> {
        let trigger_config = app
            .info
            .trigger
            .as_redis()
            .ok_or_else(|| anyhow!("Application trigger is not Redis"))?
            .clone();

        let component_triggers = app.component_triggers.try_map_values(|id, trigger| {
            trigger
                .as_redis()
                .cloned()
                .ok_or_else(|| anyhow!("Expected Redis configuration for component {}", id))
        })?;

        let subscriptions = app
            .components
            .iter()
            .enumerate()
            .filter_map(|(idx, c)| component_triggers.get(c).map(|c| (c.channel.clone(), idx)))
            .collect();

        let engine = Arc::new(builder.build().await?);

        log::trace!("Created new Redis trigger.");

        Ok(Self {
            trigger_config,
            component_triggers,
            engine,
            subscriptions,
        })
    }

    // Handle the message.
    async fn handle(&self, msg: redis::Msg) -> Result<()> {
        let channel = msg.get_channel_name();
        log::info!("Received message on channel {:?}", channel);

        if let Some(idx) = self.subscriptions.get(channel).copied() {
            let component = &self.engine.config.components[idx];
            let executor = self
                .component_triggers
                .get(component)
                .and_then(|t| t.executor.clone())
                .unwrap_or_default();

            match executor {
                spin_manifest::RedisExecutor::Spin => {
                    log::trace!("Executing Spin Redis component {}", component.id);
                    let executor = SpinRedisExecutor;
                    executor
                        .execute(
                            &self.engine,
                            &component.id,
                            channel,
                            msg.get_payload_bytes(),
                        )
                        .await?
                }
            };
        } else {
            log::debug!("No subscription found for {:?}", channel);
        }

        Ok(())
    }
}

/// The Redis executor trait.
/// All Redis executors must implement this trait.
#[async_trait]
pub(crate) trait RedisExecutor: Clone + Send + Sync + 'static {
    async fn execute(
        &self,
        engine: &ExecutionContext,
        component: &str,
        channel: &str,
        payload: &[u8],
    ) -> Result<()>;
}

#[cfg(test)]
mod tests;
