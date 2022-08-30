// @see https://www.postgresql.org/docs/current/protocol-message-formats.html

use std::ffi::CString;

use ansilo_core::err::{bail, Context, Result};
use tokio::io::{AsyncRead, AsyncWrite};

use super::common::PostgresMessage;

/// Messages recieved from the postgres frontend.
/// We only care about authentication query, and terminate messages, the rest we treat as opaque
#[derive(Debug, Clone, PartialEq)]
pub enum PostgresFrontendMessage {
    PasswordMessage(Vec<u8>),
    Query(CString),
    Terminate,
    Other(PostgresMessage),
}

impl PostgresFrontendMessage {
    /// Reads a postgres frontend message from the supplied stream
    pub async fn read(stream: &mut (impl AsyncRead + Unpin)) -> Result<Self> {
        let message = PostgresMessage::read(stream).await?;

        Ok(match message.tag() {
            b'P' | b'D' | b'E' | b'B' | b'S' | b'C' | b'd' | b'c' | b'f' => Self::Other(message),
            b'Q' => {
                Self::Query(CString::new(message.body()).context("Failed to parse query string")?)
            }
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
            Self::PasswordMessage(p) => PostgresMessage::build(b'p', |body| {
                body.write_all(p.as_slice())?;
                Ok(())
            })?,
            Self::Query(query) => PostgresMessage::build(b'Q', |body| {
                body.write_all(query.as_bytes_with_nul())?;
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

    fn to_buff(msg: PostgresFrontendMessage) -> Vec<u8> {
        msg.serialise().unwrap().into_raw()
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
            PostgresFrontendMessage::Other(PostgresMessage::new(vec![b'P', 0, 0, 0, 7, 1, 2, 3]))
        );
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_invalid_tag() {
        parse(&[b'1', 0, 0, 0, 7, 1, 2, 3]).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_proto_fe_write() {
        let msg = PostgresFrontendMessage::Other(PostgresMessage::new(vec![1, 2, 3]));

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
            to_buff(PostgresFrontendMessage::Query(
                CString::new("test").unwrap()
            )),
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
}
