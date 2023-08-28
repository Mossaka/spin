use anyhow::{anyhow, Result};
use redis::Client as RedisClient;
use reqwest::Client;
use spin_common::table::Table;
use spin_core::HostComponent;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Notify;
use wasi_http::wasi::http::types2 as types;
use wasi_messaging::wasi::messaging::messaging_types;

pub mod http;
mod keyvalue;
mod messaging;
mod poll;
mod streams;

pub mod wasi_http {
    wasmtime::component::bindgen!({
        path: "../../wit/wasi-http",
        world: "http-keyvalue",
        async: true
    });
}

pub mod wasi_messaging {
    wasmtime::component::bindgen!({
        path: "../../wit/wasi-messaging/wit",
        world: "messaging",
        async: true
    });
}

pub struct WasiCloudComponent;

impl HostComponent for WasiCloudComponent {
    type Data = WasiCloud;

    fn add_to_linker<T: Send>(
        linker: &mut spin_core::Linker<T>,
        get: impl Fn(&mut spin_core::Data<T>) -> &mut Self::Data + Send + Sync + Copy + 'static,
    ) -> anyhow::Result<()> {
        wasi_http::HttpKeyvalue::add_to_linker(linker, get)?;
        wasi_messaging::Messaging::add_to_linker(linker, get)
    }

    fn build_data(&self) -> Self::Data {
        Default::default()
    }
}

#[derive(Default)]
struct WasiMessaging {
    client_cache: HashMap<String, messaging::Client>,
    clients: Table<messaging::Client>,
    errors: Table<messaging::Error>,
}

pub struct WasiKeyvalue {
    pub client: RedisClient,
}

impl Default for WasiKeyvalue {
    fn default() -> Self {
        Self {
            client: RedisClient::open("redis://localhost:6379")
                .expect("failed to connect to redis"),
        }
    }
}

#[derive(Default)]
pub struct WasiCloud {
    incoming_requests: Table<http::IncomingRequest>,
    outgoing_responses: Table<http::OutgoingResponse>,
    outgoing_requests: Table<http::OutgoingRequest>,
    incoming_responses: Table<http::IncomingResponse>,
    future_incoming_responses: Table<http::FutureIncomingResponse>,
    future_trailers: Table<http::FutureTrailers>,
    future_write_trailers_results: Table<http::FutureWriteTrailersResult>,
    fields: Table<http::Fields>,
    response_outparams: Table<http::ResponseOutparam>,
    pollables: Table<poll::Pollable>,
    input_streams: Table<streams::InputStream>,
    output_streams: Table<streams::OutputStream>,
    notify: Arc<Notify>,
    http_client: Client,
    messaging: WasiMessaging,
    keyvalue: WasiKeyvalue,
    buckets: Table<keyvalue::Bucket>,
    keyvalue_errors: Table<keyvalue::Error>,
    outgoing_value: Table<keyvalue::OutgoingValue>,
    incoming_value: Table<keyvalue::IncomingValue>,
}

impl WasiCloud {
    pub fn push_incoming_request(
        &mut self,
        request: http::IncomingRequest,
    ) -> Result<types::IncomingRequest> {
        self.incoming_requests
            .push(request)
            .map_err(|()| anyhow!("table overflow"))
    }

    pub fn push_response_outparam(
        &mut self,
        outparam: http::ResponseOutparam,
    ) -> Result<types::ResponseOutparam> {
        self.response_outparams
            .push(outparam)
            .map_err(|()| anyhow!("table overflow"))
    }

    pub fn take_messaging_error(&mut self, error: messaging_types::Error) -> anyhow::Error {
        self.messaging
            .errors
            .remove(error)
            .unwrap_or_else(|| anyhow!("unknown handle: {error}"))
    }
}
