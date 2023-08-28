use anyhow::anyhow;
use redis::AsyncCommands;
use spin_core::async_trait;

use crate::{
    wasi_http::{
        self,
        wasi::http::{
            readwrite,
            readwrite::Key,
            types_keyvalue::{IncomingValueAsyncBody, IncomingValueSyncBody, OutputStream},
        },
    },
    WasiCloud,
};

pub struct Bucket {
    name: String,
}

pub struct Error {
    message: String,
}

#[derive(Default)]
pub struct OutgoingValue {
    body: Vec<u8>,
}

#[derive(Default)]
pub struct IncomingValue {
    body: Vec<u8>,
}

#[async_trait]
impl crate::wasi_http::wasi::http::readwrite::Host for WasiCloud {
    async fn get(
        &mut self,
        bucket: readwrite::Bucket,
        key: Key,
    ) -> Result<Result<readwrite::IncomingValue, readwrite::Error>, anyhow::Error> {
        let name = self
            .buckets
            .get(bucket)
            .ok_or_else(|| anyhow!("unknown handle: {bucket}"))?
            .name
            .clone();
        let mut conn = self.keyvalue.client.get_async_connection().await?;
        let value: Vec<u8> = conn.get(form_key(&name, &key)).await?;
        let incoming_value = self
            .incoming_value
            .push(IncomingValue { body: value })
            .map_err(|_| {
                let error_resource = self
                    .keyvalue_errors
                    .push(Error {
                        message: "table overflow".into(),
                    })
                    .unwrap();
                error_resource
            });
        Ok(incoming_value)
    }

    async fn set(
        &mut self,
        bucket: readwrite::Bucket,
        key: Key,
        outgoing_value: readwrite::OutgoingValue,
    ) -> Result<Result<(), readwrite::Error>, anyhow::Error> {
        let name = self
            .buckets
            .get(bucket)
            .ok_or_else(|| anyhow!("unknown handle: {bucket}"))?
            .name
            .clone();
        let value = self
            .outgoing_value
            .get_mut(outgoing_value)
            .ok_or_else(|| anyhow!("unknown handle: {outgoing_value}"))?
            .body
            .clone();
        let mut conn = self.keyvalue.client.get_async_connection().await?;
        conn.set(form_key(&name, &key), value).await?;
        Ok(Ok(()))
    }

    async fn delete(
        &mut self,
        bucket: readwrite::Bucket,
        key: Key,
    ) -> Result<Result<(), readwrite::Error>, anyhow::Error> {
        let name = self
            .buckets
            .get(bucket)
            .ok_or_else(|| anyhow!("unknown handle: {bucket}"))?
            .name
            .clone();
        let mut conn = self.keyvalue.client.get_async_connection().await?;
        conn.del(form_key(&name, &key)).await?;
        Ok(Ok(()))
    }

    async fn exists(
        &mut self,
        bucket: readwrite::Bucket,
        key: Key,
    ) -> Result<Result<bool, readwrite::Error>, anyhow::Error> {
        let name = self
            .buckets
            .get(bucket)
            .ok_or_else(|| anyhow!("unknown handle: {bucket}"))?
            .name
            .clone();
        let mut conn = self.keyvalue.client.get_async_connection().await?;
        let exists: bool = conn.exists(form_key(&name, &key)).await?;
        Ok(Ok(exists))
    }
}

#[async_trait]
impl wasi_http::wasi::http::wasi_cloud_error::Host for WasiCloud {
    async fn drop_error(&mut self, error: readwrite::Error) -> Result<(), anyhow::Error> {
        self.keyvalue_errors
            .remove(error)
            .map(drop)
            .ok_or_else(|| anyhow!("unknown handle: {error}", error = error))
    }

    async fn trace(&mut self, error: readwrite::Error) -> Result<String, anyhow::Error> {
        Ok(self
            .keyvalue_errors
            .get(error)
            .ok_or_else(|| anyhow!("unknown handle: {error}"))?
            .message
            .clone())
    }
}

#[async_trait]
impl wasi_http::wasi::http::types_keyvalue::Host for WasiCloud {
    async fn drop_bucket(&mut self, bucket: readwrite::Bucket) -> Result<(), anyhow::Error> {
        self.buckets
            .remove(bucket)
            .map(drop)
            .ok_or_else(|| anyhow!("unknown handle: {bucket}"))
    }

    async fn open_bucket(
        &mut self,
        name: String,
    ) -> Result<Result<readwrite::Bucket, readwrite::Error>, anyhow::Error> {
        let bucket_resource = self.buckets.push(Bucket { name }).map_err(|_| {
            let error_resource = self
                .keyvalue_errors
                .push(Error {
                    message: "table overflow".into(),
                })
                .unwrap();
            error_resource
        });
        Ok(bucket_resource)
    }

    async fn drop_outgoing_value(
        &mut self,
        outgoing_value: readwrite::OutgoingValue,
    ) -> Result<(), anyhow::Error> {
        self.outgoing_value
            .remove(outgoing_value)
            .map(drop)
            .ok_or_else(|| anyhow!("unknown handle: {outgoing_value}"))
    }

    async fn new_outgoing_value(&mut self) -> Result<readwrite::OutgoingValue, anyhow::Error> {
        let value = self
            .outgoing_value
            .push(OutgoingValue::default())
            .map_err(|_| anyhow!("table overflow"))?;
        Ok(value)
    }

    async fn outgoing_value_write_body_async(
        &mut self,
        _outgoing_value: readwrite::OutgoingValue,
    ) -> Result<Result<OutputStream, readwrite::Error>, anyhow::Error> {
        todo!()
    }

    async fn outgoing_value_write_body_sync(
        &mut self,
        outgoing_value: readwrite::OutgoingValue,
        value: Vec<u8>,
    ) -> Result<Result<(), readwrite::Error>, anyhow::Error> {
        self.outgoing_value
            .get_mut(outgoing_value)
            .ok_or_else(|| anyhow!("unknown handle: {outgoing_value}"))?
            .body
            .extend(value);
        Ok(Ok(()))
    }

    async fn drop_incoming_value(
        &mut self,
        incoming_value: readwrite::IncomingValue,
    ) -> Result<(), anyhow::Error> {
        self.incoming_value
            .remove(incoming_value)
            .map(drop)
            .ok_or_else(|| {
                anyhow!(
                    "unknown handle: {incoming_value}",
                    incoming_value = incoming_value
                )
            })
    }

    async fn incoming_value_consume_sync(
        &mut self,
        incoming_value: readwrite::IncomingValue,
    ) -> Result<Result<IncomingValueSyncBody, readwrite::Error>, anyhow::Error> {
        let body = self
            .incoming_value
            .get_mut(incoming_value)
            .ok_or_else(|| anyhow!("unknown handle: {incoming_value}"))?
            .body
            .clone();
        Ok(Ok(body))
    }

    async fn incoming_value_consume_async(
        &mut self,
        _incoming_value: readwrite::IncomingValue,
    ) -> Result<Result<IncomingValueAsyncBody, readwrite::Error>, anyhow::Error> {
        todo!()
    }

    async fn size(
        &mut self,
        incoming_value: readwrite::IncomingValue,
    ) -> Result<u64, anyhow::Error> {
        Ok(self
            .incoming_value
            .get_mut(incoming_value)
            .ok_or_else(|| anyhow!("unknown handle: {incoming_value}"))?
            .body
            .len() as u64)
    }
}

fn form_key(bucket: &str, key: &str) -> String {
    format!("{}:{}", bucket, key)
}
