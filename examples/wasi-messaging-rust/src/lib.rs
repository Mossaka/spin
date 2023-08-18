wit_bindgen::generate!("messaging" in "../../wit/wasi-messaging/wit");

use {
    crate::{
        exports::wasi::messaging::messaging_guest::MessagingGuest,
        wasi::messaging::{
            messaging_types::{self, Error, FormatSpec, GuestConfiguration, Message},
            producer,
        },
    },
    std::env,
};

fn redis_address() -> String {
    env::var("REDIS_ADDRESS").expect("REDIS_ADDRESS should be a valid UTF-8 environment variable")
}

struct Component;

impl MessagingGuest for Component {
    fn configure() -> Result<GuestConfiguration, Error> {
        Ok(GuestConfiguration {
            service: redis_address(),
            channels: vec!["foo".to_owned()],
            extensions: None,
        })
    }

    fn handler(messages: Vec<Message>) -> Result<(), Error> {
        for Message {
            data,
            format,
            metadata,
        } in &messages
        {
            assert_eq!(format, &FormatSpec::Raw);

            if let Some(metadata) = metadata {
                assert_eq!(1, metadata.len());
                assert_eq!("channel", &metadata[0].0);
                assert_eq!("foo", &metadata[0].1);
            } else {
                panic!("expected channel name in metadata");
            }

            match data.as_slice() {
                b"first" => producer::send(
                    messaging_types::connect(&redis_address())?,
                    &"foo".to_string(),
                    &[&Message {
                        data: b"second".to_vec(),
                        format: FormatSpec::Raw,
                        metadata: None,
                    }],
                )?,

                b"second" => producer::send(
                    messaging_types::connect(&redis_address())?,
                    &"foo".to_string(),
                    &[&Message {
                        data: b"third".to_vec(),
                        format: FormatSpec::Raw,
                        metadata: None,
                    }],
                )?,

                b"third" => (),

                _ => panic!("unexpected message: {}", String::from_utf8_lossy(data)),
            }
        }

        Ok(())
    }
}

export_messaging!(Component);
