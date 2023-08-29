use url::Url;

use crate::wasi::http::{
    readwrite::{delete, exists, get, set},
    types_keyvalue::{
        drop_bucket, incoming_value_consume_sync, new_outgoing_value,
        outgoing_value_write_body_sync, Bucket,
    },
    wasi_cloud_error::{drop_error, trace},
};

wit_bindgen::generate!({
    world: "http-keyvalue",
    path: "../../wit/wasi-http",
    exports: {
        "wasi:http/incoming-handler2": Component
    },
});

use {
    self::{
        exports::wasi::http::incoming_handler2::Guest as IncomingHandler,
        wasi::{
            http::{
                types2::{self as types, IncomingRequest, Method, ResponseOutparam},
                types_keyvalue::open_bucket,
            },
            io::streams2::{self as streams, StreamStatus},
        },
    },
    anyhow::{anyhow, Error, Result},
    futures::{future, sink, stream, Sink, SinkExt, Stream, TryStreamExt},
    std::{str, task::Poll},
    wakers::Wakers,
};

mod wakers;

const READ_SIZE: u64 = 16 * 1024;

struct Component;

impl IncomingHandler for Component {
    fn handle(request: IncomingRequest, response_out: ResponseOutparam) {
        let wakers = Wakers::default();
        let future = handle_async(wakers.clone(), request, response_out);
        futures::pin_mut!(future);
        wakers.run(future).unwrap();
    }
}

async fn handle_async(
    wakers: Wakers,
    request: IncomingRequest,
    response_out: ResponseOutparam,
) -> Result<()> {
    let method = types::incoming_request_method(request);
    let path = types::incoming_request_path_with_query(request);

    if path.is_none() {
        let response = types::new_outgoing_response(405, types::new_fields(&[]))?;

        types::set_response_outparam(response_out, Ok(response))
            .expect("response outparam should be settable");

        types::finish_outgoing_stream(
            types::outgoing_response_write(response).expect("response should be writable"),
        );
        return Ok(());
    }
    let parsed_url = url::Url::parse(&format!("http://dummy.com{}", path.clone().unwrap()))?;
    let path_segments = parsed_url.path();
    let query_pairs = parsed_url.query_pairs();

    match (method, path_segments) {
        (Method::Put, "/set") => {
            let bucket = get_bucket(query_pairs)?;

            let mut stream = incoming_request_body(wakers.clone(), request);
            let mut request_data = Vec::new();
            while let Some(chunk) = stream.try_next().await? {
                request_data.extend(chunk);
            }

            // Convert request_data to a String
            let data_str = String::from_utf8(request_data)?;

            // Parse the key-value pair
            let mut iter = data_str.splitn(2, '=');
            let key = iter.next().ok_or_else(|| anyhow!("Missing key"))?;
            let value = iter.next().ok_or_else(|| anyhow!("Missing value"))?;

            // Insert the key-value pair into the bucket
            let outgoing_value = new_outgoing_value();
            outgoing_value_write_body_sync(outgoing_value, value.as_bytes()).map_err(|err| {
                let err_message = trace(err);
                drop_error(err);
                anyhow!("Failed to write value: {err_message}")
            })?;

            set(bucket, &key.to_owned(), outgoing_value).map_err(|err| {
                let err_message = trace(err);
                drop_error(err);
                anyhow!("Failed to set value: {err_message}")
            })?;

            let response = types::new_outgoing_response(200, types::new_fields(&[]))?;
            types::set_response_outparam(response_out, Ok(response))
                .expect("response outparam should be settable");
            let mut sink = outgoing_response_body(wakers, response);
            sink.send(None).await?;
            drop_bucket(bucket);
        }

        (Method::Get, "/get") => {
            let bucket = get_bucket(query_pairs)?;
            let key = get_key(query_pairs)?;

            let val = get(bucket, &key).map_err(|err| {
                let err_message = trace(err);
                drop_error(err);
                anyhow!("Failed to get value: {err_message}")
            })?;

            let value = incoming_value_consume_sync(val).map_err(|err| {
                let err_message = trace(err);
                drop_error(err);
                anyhow!("Failed to consume value: {err_message}")
            })?;

            let response = types::new_outgoing_response(
                200,
                types::new_fields(&[("content-type".to_string(), b"text/plain".to_vec())]),
            )?;
            types::set_response_outparam(response_out, Ok(response))
                .expect("response outparam should be settable");
            let mut sink = outgoing_response_body(wakers, response);
            sink.send(Some(value)).await?;
            sink.send(None).await?;
        }

        (Method::Get, "/exists") => {
            let bucket = get_bucket(query_pairs)?;
            let key = get_key(query_pairs)?;

            let is_exists = exists(bucket, &key).map_err(|err| {
                let err_message = trace(err);
                drop_error(err);
                anyhow!("Failed to check if value exists: {err_message}")
            })?;

            let response = types::new_outgoing_response(
                if is_exists { 204 } else { 404 },
                types::new_fields(&[]),
            )?;
            types::set_response_outparam(response_out, Ok(response))
                .expect("response outparam should be settable");
            let mut sink = outgoing_response_body(wakers, response);
            sink.send(None).await?;
        }

        (Method::Delete, "/delete") => {
            let bucket = get_bucket(query_pairs)?;
            let key = get_key(query_pairs)?;

            delete(bucket, &key).map_err(|err| {
                let err_message = trace(err);
                drop_error(err);
                anyhow!("Failed to delete value: {err_message}")
            })?;

            let response = types::new_outgoing_response(204, types::new_fields(&[]))?;

            types::set_response_outparam(response_out, Ok(response))
                .expect("response outparam should be settable");
            let mut sink = outgoing_response_body(wakers, response);
            sink.send(None).await?;
        }

        _ => {
            let response = types::new_outgoing_response(405, types::new_fields(&[]))?;

            types::set_response_outparam(response_out, Ok(response))
                .expect("response outparam should be settable");

            types::finish_outgoing_stream(
                types::outgoing_response_write(response).expect("response should be writable"),
            );
        }
    }

    Ok(())
}

fn get_bucket(query_pairs: url::form_urlencoded::Parse<'_>) -> Result<u32, Error> {
    let bucket_name = query_pairs
        .clone()
        .find(|(k, _)| k == "bucket_name")
        .map(|(_, v)| v.into_owned())
        .ok_or_else(|| anyhow!("Missing or invalid 'bucket_name' query parameter"))?;
    println!("bucket name: {bucket_name}");
    let bucket = open_bucket(&bucket_name).map_err(|err| {
        let err_message = trace(err);
        drop_error(err);
        anyhow!("Failed to open bucket: {err_message}")
    })?;
    Ok(bucket)
}

fn get_key(query_pairs: url::form_urlencoded::Parse<'_>) -> Result<String, Error> {
    let key = query_pairs
        .clone()
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.into_owned())
        .ok_or_else(|| anyhow!("Missing or invalid 'key' query parameter"))?;
    Ok(key)
}

fn outgoing_response_body(
    wakers: Wakers,
    response: types::OutgoingResponse,
) -> impl Sink<Option<Vec<u8>>, Error = Error> {
    outgoing_body(
        wakers,
        types::outgoing_response_write(response).expect("response should be writable"),
    )
}

fn outgoing_body(
    wakers: Wakers,
    body: streams::OutputStream,
) -> impl Sink<Option<Vec<u8>>, Error = Error> {
    sink::unfold((), {
        let pollable = streams::subscribe_to_output_stream(body);

        move |(), chunk: Option<Vec<u8>>| {
            future::poll_fn({
                let mut offset = 0;
                let wakers = wakers.clone();

                move |context| {
                    if let Some(chunk) = chunk.as_ref() {
                        assert!(!chunk[offset..].is_empty());

                        match streams::write(body, &chunk[offset..]) {
                            Ok(count) => {
                                let count = usize::try_from(count).unwrap();
                                offset += count;
                                if offset == chunk.len() {
                                    Poll::Ready(Ok(()))
                                } else {
                                    wakers.insert(pollable, context.waker().clone());
                                    Poll::Pending
                                }
                            }
                            Err(_) => Poll::Ready(Err(anyhow!("I/O error"))),
                        }
                    } else {
                        types::finish_outgoing_stream(body);
                        Poll::Ready(Ok(()))
                    }
                }
            })
        }
    })
}

fn incoming_request_body(
    wakers: Wakers,
    request: types::IncomingRequest,
) -> impl Stream<Item = Result<Vec<u8>>> {
    incoming_body(
        wakers,
        types::incoming_request_consume(request).expect("request should be consumable"),
    )
}

fn incoming_body(wakers: Wakers, body: types::InputStream) -> impl Stream<Item = Result<Vec<u8>>> {
    stream::poll_fn({
        let pollable = streams::subscribe_to_input_stream(body);
        let mut saw_end = false;

        move |context| {
            if saw_end {
                Poll::Ready(None)
            } else {
                match streams::read(body, READ_SIZE) {
                    Ok((buffer, status)) => {
                        if let StreamStatus::Ended = status {
                            types::finish_incoming_stream(body);
                            saw_end = true;
                        }

                        if buffer.is_empty() {
                            if let StreamStatus::Ended = status {
                                Poll::Ready(None)
                            } else {
                                wakers.insert(pollable, context.waker().clone());
                                Poll::Pending
                            }
                        } else {
                            Poll::Ready(Some(Ok(buffer)))
                        }
                    }
                    Err(_) => Poll::Ready(Some(Err(anyhow!("I/O error")))),
                }
            }
        }
    })
}
