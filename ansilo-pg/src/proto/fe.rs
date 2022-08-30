// @see https://www.postgresql.org/docs/current/protocol-message-formats.html

use std::{collections::HashMap, ffi::CString};

use ansilo_core::err::{bail, ensure, Context, Result};
use tokio::io::{AsyncRead, AsyncWrite};

use super::common::PostgresMessage;

const PG_PROTO_VERSION: i32 = 196608;

/// Messages recieved from the postgres frontend.
/// We only care about authentication query, and terminate messages, the rest we treat as opaque
#[derive(Debug, Clone, PartialEq)]
pub enum PostgresFrontendMessage {
    StartupMessage(PostgresFrontendStartupMessage),
    PasswordMessage(Vec<u8>),
    Query(String),
    Terminate,
    Other(PostgresMessage),
}

/// Postgres frontend startup message
#[derive(Debug, Clone, PartialEq)]
pub struct PostgresFrontendStartupMessage {
    /// The protocol version number (196608 for v3)
    protocol_version: i32,
    /// Parameters
    params: HashMap<String, String>,
}

impl PostgresFrontendStartupMessage {
    pub fn new(params: HashMap<String, String>) -> Self {
        Self {
            protocol_version: PG_PROTO_VERSION,
            params,
        }
    }
}

impl PostgresFrontendMessage {
    /// Reads a postgres startup message from the supplied stream
    pub async fn read_startup(
        stream: &mut (impl AsyncRead + Unpin),
    ) -> Result<PostgresFrontendStartupMessage> {
        let message = PostgresMessage::read_untagged(stream).await?;

        ensure!(message.body_length() >= 4, "Invalid startup message length");

        let protocol_version = i32::from_be_bytes(message.body()[..4].try_into().unwrap());
        ensure!(
            protocol_version == PG_PROTO_VERSION,
            "Unexpected protocol version"
        );

        let body = message.body();

        // Remove last null terminator
        ensure!(
            *body.last().unwrap() == 0,
            "Startup message body was not null terminated"
        );

        let strings = body[4..body.len() - 2]
            .split(|i| *i == 0)
            .map(|s| {
                String::from_utf8(s.to_vec()).context("Failed to parse startup message string")
            })
            .collect::<Result<Vec<_>>>()?;

        ensure!(
            strings.len() % 2 == 0,
            "Invalid number of strings found in startup message"
        );

        let params = strings
            .chunks_exact(2)
            .map(|c| (c[0].clone(), c[1].clone()))
            .collect();

        Ok(PostgresFrontendStartupMessage {
            protocol_version,
            params,
        })
    }

    /// Reads a postgres frontend message from the supplied stream
    pub async fn read(stream: &mut (impl AsyncRead + Unpin)) -> Result<Self> {
        let message = PostgresMessage::read(stream).await?;

        Ok(match message.tag() {
            b'P' | b'D' | b'E' | b'B' | b'S' | b'C' | b'd' | b'c' | b'f' => Self::Other(message),
            b'Q' => Self::Query(
                String::from_utf8(message.body().to_vec())
                    .context("Failed to parse query string")?,
            ),
            b'X' => Self::Terminate,
            b'p' => Self::PasswordMessage(message.body().to_vec()),
            _ => bail!("Unknown postgres frontend message: {:?}", message),
        })
    }

    /// Writes the message to the supplied stream
    pub async fn write(self, stream: &mut (impl AsyncWrite + Unpin)) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let msg = self.serialise()?;

        stream
            .write_all(msg.as_slice())
            .await
            .context("Failed to write postgres backend message")?;

        Ok(())
    }

    /// Serialises the message into a message format that can be sent to postgres.
    pub fn serialise(self) -> Result<PostgresMessage> {
        use std::io::Write;

        Ok(match self {
            Self::Other(m) => m,
            Self::StartupMessage(msg) => PostgresMessage::build_untagged(|body| {
                body.write_all(msg.protocol_version.to_be_bytes().as_slice())?;

                for string in msg.params.into_iter().flat_map(|(k, v)| [k, v]) {
                    body.write_all(CString::new(string)?.as_bytes_with_nul())?;
                }

                body.write_all(&[0])?;

                Ok(())
            })?,
            Self::PasswordMessage(p) => PostgresMessage::build(b'p', |body| {
                body.write_all(p.as_slice())?;
                Ok(())
            })?,
            Self::Query(query) => PostgresMessage::build(b'Q', |body| {
                body.write_all(CString::new(query)?.as_bytes_with_nul())?;
                Ok(())
            })?,
            Self::Terminate => PostgresMessage::build(b'X', |_| Ok(()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio_test::io::Builder;

    use super::*;

    async fn parse(buf: &[u8]) -> Result<PostgresFrontendMessage> {
        let mut stream = Builder::new().read(buf).build();
        PostgresFrontendMessage::read(&mut stream).await
    }

    async fn parse_startup(buf: &[u8]) -> Result<PostgresFrontendStartupMessage> {
        let mut stream = Builder::new().read(buf).build();
        PostgresFrontendMessage::read_startup(&mut stream).await
    }

    fn to_buff(msg: PostgresFrontendMessage) -> Vec<u8> {
        msg.serialise().unwrap().into_raw()
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_startup_message() {
        let parsed = parse_startup(&[
            0, 0, 0, 21, //len
            0, 3, 0, 0, // protocol version
            b'k', b'1', 0, // key 1
            b'v', b'1', 0, // value 1
            b'k', b'2', 0, // key 2
            b'v', b'2', 0, // value 2
            0, // terminator
        ])
        .await
        .unwrap();

        assert_eq!(
            parsed,
            PostgresFrontendStartupMessage {
                protocol_version: 196608,
                params: [("k1".into(), "v1".into()), ("k2".into(), "v2".into()),]
                    .into_iter()
                    .collect()
            }
        );
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_password_manage() {
        let parsed = parse(&[b'p', 0, 0, 0, 5, 1]).await.unwrap();

        assert_eq!(parsed, PostgresFrontendMessage::PasswordMessage(vec![1]));
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_terminate() {
        let parsed = parse(&[b'X', 0, 0, 0, 4]).await.unwrap();

        assert_eq!(parsed, PostgresFrontendMessage::Terminate);
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_other() {
        let parsed = parse(&[b'P', 0, 0, 0, 7, 1, 2, 3]).await.unwrap();

        assert_eq!(
            parsed,
            PostgresFrontendMessage::Other(PostgresMessage::Tagged(vec![
                b'P', 0, 0, 0, 7, 1, 2, 3
            ]))
        );
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_invalid_tag() {
        parse(&[b'1', 0, 0, 0, 7, 1, 2, 3]).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_proto_fe_write() {
        let msg = PostgresFrontendMessage::Other(PostgresMessage::Tagged(vec![1, 2, 3]));

        let mut stream = Builder::new().write(&[1, 2, 3]).build();

        msg.write(&mut stream).await.unwrap();
    }

    #[test]
    fn test_proto_fe_message_serialise_password_message() {
        assert_eq!(
            to_buff(PostgresFrontendMessage::PasswordMessage(vec![1, 2, 3, 0])),
            vec![
                b'p', // tag
                0, 0, 0, 8, // len
                1, 2, 3, 0, // content
            ]
        );
    }

    #[test]
    fn test_proto_fe_message_serialise_query() {
        assert_eq!(
            to_buff(PostgresFrontendMessage::Query("test".into())),
            vec![
                b'Q', // tag
                0, 0, 0, 9, // len
                b't', b'e', b's', b't', 0, // content
            ]
        );
    }

    #[test]
    fn test_proto_fe_message_serialise_terminate() {
        assert_eq!(
            to_buff(PostgresFrontendMessage::Terminate),
            vec![
                b'X', // tag
                0, 0, 0, 4, // len
            ]
        );
    }

    #[test]
    fn test_proto_fe_message_serialise_startup_message() {
        let buff = to_buff(PostgresFrontendMessage::StartupMessage(
            PostgresFrontendStartupMessage {
                protocol_version: 196608,
                params: [("k1".into(), "v1".into()), ("k2".into(), "v2".into())]
                    .into_iter()
                    .collect(),
            },
        ));

        assert_eq!(
            buff[..8],
            vec![
                0, 0, 0, 21, //len
                0, 3, 0, 0, // protocol version
            ]
        );

        // hash map ordering is non-determistic
        if buff[9] == b'1' {
            assert_eq!(
                buff[8..],
                vec![
                    b'k', b'1', 0, // key 1
                    b'v', b'1', 0, // value 1
                    b'k', b'2', 0, // key 2
                    b'v', b'2', 0, // value 2
                    0  // terminator
                ]
            );
        } else {
            assert_eq!(
                buff[8..],
                vec![
                    b'k', b'2', 0, // key 2
                    b'v', b'2', 0, // value 2
                    b'k', b'1', 0, // key 1
                    b'v', b'1', 0, // value 1
                    0  // terminator
                ]
            );
        }
    }
}
