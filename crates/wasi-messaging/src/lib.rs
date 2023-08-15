use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use redis::{Client, ConnectionLike};
use serde::{Deserialize, Serialize};
use spin_app::MetadataKey;
use spin_core::async_trait;
use spin_trigger::{cli::NoArgs, EitherInstance, TriggerAppEngine, TriggerExecutor};
use std::collections::{HashMap, HashSet};
use wasi_cloud::wasi_messaging::{
    wasi::messaging::messaging_types::{Error, FormatSpec, Message},
    Messaging,
};

const TRIGGER_METADATA_KEY: MetadataKey<TriggerMetadata> = MetadataKey::new("trigger");

pub(crate) type RuntimeData = ();
pub(crate) type Store = spin_core::Store<RuntimeData>;

pub struct WasiMessagingTrigger {
    engine: TriggerAppEngine<Self>,
    address: String,
    components: HashSet<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WasiMessagingTriggerConfig {
    component: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TriggerMetadata {
    r#type: String,
    address: String,
}

#[async_trait]
impl TriggerExecutor for WasiMessagingTrigger {
    const TRIGGER_TYPE: &'static str = "wasi-messaging";
    type RuntimeData = RuntimeData;
    type TriggerConfig = WasiMessagingTriggerConfig;
    type RunConfig = NoArgs;

    async fn new(engine: TriggerAppEngine<Self>) -> Result<Self> {
        let address = engine.app().require_metadata(TRIGGER_METADATA_KEY)?.address;

        let components = engine
            .trigger_configs()
            .map(|(_, config)| config.component.clone())
            .collect();

        Ok(Self {
            engine,
            address,
            components,
        })
    }

    async fn run(self, _config: Self::RunConfig) -> Result<()> {
        let address = &self.address;

        tracing::info!("Connecting to Redis server at {address}");
        let mut client = Client::open(address.to_string())?;
        let mut pubsub = client
            .get_async_connection()
            .await
            .with_context(|| anyhow!("Redis trigger failed to connect to {address}"))?
            .into_pubsub();

        let mut components = HashMap::<_, HashSet<_>>::new();
        for component in self.components.iter() {
            let (instance, mut store) = self.engine.prepare_instance(component).await?;
            let EitherInstance::Component(instance) = instance else {
                unreachable!()
            };

            let messaging = Messaging::new(&mut store, &instance)?;

            let config = messaging
                .wasi_messaging_messaging_guest()
                .call_configure(&mut store)
                .await?;

            let config = check_error(&mut store, &self.engine, config)?;

            if config.extensions.is_some() {
                bail!("`wasi-messaging` guest configuration extensions not yet supported");
            }

            for channel in config.channels {
                components.entry(channel).or_default().insert(component);
            }
        }

        for channel in components.keys() {
            tracing::info!("Subscribing to channel {channel:?}");
            pubsub.subscribe(channel).await?;
        }

        let empty = &HashSet::new();

        let mut stream = pubsub.on_message();
        loop {
            match stream.next().await {
                Some(msg) => {
                    let channel = msg.get_channel_name();
                    for component in components.get(channel).unwrap_or(empty) {
                        let (instance, mut store) = self.engine.prepare_instance(component).await?;
                        let EitherInstance::Component(instance) = instance else {
                            unreachable!()
                        };

                        let messaging = Messaging::new(&mut store, &instance)?;

                        let result = messaging
                            .wasi_messaging_messaging_guest()
                            .call_handler(
                                &mut store,
                                &[&Message {
                                    data: msg.get_payload_bytes().to_owned(),
                                    format: FormatSpec::Raw,
                                    metadata: Some(vec![(
                                        "channel".to_owned(),
                                        channel.to_owned(),
                                    )]),
                                }],
                            )
                            .await?;

                        check_error(&mut store, &self.engine, result)?;
                    }
                }
                None => {
                    tracing::trace!("Empty message");
                    if !client.check_connection() {
                        tracing::info!("No Redis connection available");
                        break Ok(());
                    }
                }
            };
        }
    }
}

fn check_error<T>(
    store: &mut Store,
    engine: &TriggerAppEngine<WasiMessagingTrigger>,
    result: Result<T, Error>,
) -> Result<T> {
    match result {
        Ok(result) => Ok(result),
        Err(e) => {
            let cloud =
                store
                    .host_components_data()
                    .get_or_insert(engine.wasi_cloud().ok_or_else(|| {
                        anyhow!("WasiMessagingTrigger needs access to `wasi-cloud` host component")
                    })?);

            Err(cloud.take_messaging_error(e))
        }
    }
}
