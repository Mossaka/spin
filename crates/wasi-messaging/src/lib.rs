use anyhow::{anyhow, bail, Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use futures::{future, stream::FuturesUnordered, SinkExt, TryStreamExt};
use redis_protocol::resp3::{decode, encode, types::Frame};
use serde::{Deserialize, Serialize};
use spin_core::async_trait;
use spin_trigger::{cli::NoArgs, EitherInstance, TriggerAppEngine, TriggerExecutor};
use std::{collections::HashSet, ops::Deref, str};
use tokio::{net::TcpStream, task};
use tokio_util::codec::{Decoder, Encoder, Framed};
use url::Url;
use wasi_cloud::wasi_messaging::{
    wasi::messaging::messaging_types::{Error, FormatSpec, Message},
    Messaging,
};

pub(crate) type RuntimeData = ();
pub(crate) type Store = spin_core::Store<RuntimeData>;

pub struct WasiMessagingTrigger {
    engine: TriggerAppEngine<Self>,
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
}

pub struct RedisCodec;

impl Decoder for RedisCodec {
    type Item = Frame;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>> {
        let bytes = Bytes::copy_from_slice(src);
        Ok(
            if let Some((frame, length)) =
                decode::complete::decode(&bytes).map_err(|e| anyhow!("{e}"))?
            {
                src.advance(length);
                Some(frame)
            } else {
                None
            },
        )
    }
}

impl Encoder<Frame> for RedisCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<()> {
        let mut buffer = vec![0; 256];
        let length =
            encode::complete::encode(&mut buffer, 0, &frame).map_err(|e| anyhow!("{e}"))?;
        dst.extend_from_slice(&buffer[..length]);
        Ok(())
    }
}

#[async_trait]
impl TriggerExecutor for WasiMessagingTrigger {
    const TRIGGER_TYPE: &'static str = "wasi-messaging";
    type RuntimeData = RuntimeData;
    type TriggerConfig = WasiMessagingTriggerConfig;
    type RunConfig = NoArgs;

    async fn new(engine: TriggerAppEngine<Self>) -> Result<Self> {
        let components = engine
            .trigger_configs()
            .map(|(_, config)| config.component.clone())
            .collect();

        Ok(Self { engine, components })
    }

    async fn run(self, _config: Self::RunConfig) -> Result<()> {
        self.components
            .iter()
            .map(|component| self.run_component(component))
            .collect::<FuturesUnordered<_>>()
            .try_for_each(|_| future::ok(()))
            .await
    }
}

impl WasiMessagingTrigger {
    async fn run_component(&self, component: &str) -> Result<()> {
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

        tracing::info!("Connecting to Redis server at {}", config.service);
        let url = Url::parse(&config.service)
            .with_context(|| format!("unable to parse {} as URL", config.service))?;
        if url.scheme() != "redis" {
            bail!("unsupported URL scheme: {}", url.scheme());
        }
        let host = url.host_str().unwrap_or("<unknown>");

        let address = task::block_in_place(|| url.socket_addrs(|| Some(6379)))
            .with_context(|| format!("unable to resolve {host}"))?
            .first()
            .copied()
            .ok_or_else(|| anyhow!("unable to resolve {host}"))?;

        let mut connection = Framed::new(
            TcpStream::connect(address)
                .await
                .with_context(|| format!("unable to connect to {host}"))?,
            RedisCodec,
        );

        for channel in config.channels {
            tracing::info!("Subscribing to channel {channel:?}");

            connection
                .send(Frame::Array {
                    data: vec![
                        Frame::BlobString {
                            data: Bytes::copy_from_slice(b"SUBSCRIBE"),
                            attributes: None,
                        },
                        Frame::BlobString {
                            data: Bytes::copy_from_slice(channel.as_bytes()),
                            attributes: None,
                        },
                    ],
                    attributes: None,
                })
                .await?;
        }

        while let Some(frame) = connection.try_next().await? {
            let unexpected = || Err(anyhow!("don't know how to handle frame: {frame:?}"));

            match &frame {
                Frame::Array { data, .. } => match data.as_slice() {
                    [Frame::BlobString { data, .. }, Frame::BlobString { .. }, Frame::Number { .. }]
                        if data.deref() == b"subscribe" => {}

                    [Frame::BlobString { data: data1, .. }, Frame::BlobString { data: data2, .. }, Frame::BlobString { data: data3, .. }] => {
                        match (data1.deref(), data2.deref(), data3.deref()) {
                            (b"message", channel, body) => {
                                let channel = str::from_utf8(channel)?;
                                tracing::trace!(
                                    "got message on channel {channel}: {}",
                                    String::from_utf8_lossy(body)
                                );

                                let (instance, mut store) =
                                    self.engine.prepare_instance(component).await?;

                                let EitherInstance::Component(instance) = instance else {
                                    unreachable!()
                                };

                                let messaging = Messaging::new(&mut store, &instance)?;

                                let result = messaging
                                    .wasi_messaging_messaging_guest()
                                    .call_handler(
                                        &mut store,
                                        &[&Message {
                                            data: body.to_owned(),
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
                            _ => return unexpected(),
                        }
                    }
                    _ => return unexpected(),
                },
                _ => return unexpected(),
            }
        }

        Ok(())
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
