// @see https://www.postgresql.org/docs/current/protocol-message-formats.html

use ansilo_core::err::{bail, Result};
use tokio::io::AsyncRead;

use super::common::PostgresMessage;

/// Messages recieved from the postgres frontend.
/// We only care about authentication and close messages, the rest we treat as opaque
#[derive(Debug, Clone, PartialEq)]
pub enum PostgresFrontendMessage {
    PasswordMessage(Vec<u8>),
    Close,
    Other(PostgresMessage),
}

impl PostgresFrontendMessage {
    /// Reads a postgres frontend message from the supplied stream
    pub async fn read(stream: &mut (impl AsyncRead + Unpin)) -> Result<Self> {
        let message = PostgresMessage::read(stream).await?;

        Ok(match message.tag() {
            b'Q' | b'P' | b'D' | b'E' | b'B' | b'S' | b'X' | b'd' | b'c' | b'f' => {
                Self::Other(message)
            }
            b'C' => Self::Close,
            b'p' => Self::PasswordMessage(message.body().to_vec()),
            _ => bail!("Unknown postgres frontend message: {:?}", message),
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio_test::io::Builder;

    use super::*;

    async fn test_parse(buf: &[u8]) -> Result<PostgresFrontendMessage> {
        let mut stream = Builder::new().read(buf).build();
        PostgresFrontendMessage::read(&mut stream).await
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_password_manage() {
        let parsed = test_parse(&[b'p', 0, 0, 0, 5, 1]).await.unwrap();

        assert_eq!(parsed, PostgresFrontendMessage::PasswordMessage(vec![1]));
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_close() {
        let parsed = test_parse(&[b'C', 0, 0, 0, 4]).await.unwrap();

        assert_eq!(parsed, PostgresFrontendMessage::Close);
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_other() {
        let parsed = test_parse(&[b'P', 0, 0, 0, 7, 1, 2, 3]).await.unwrap();

        assert_eq!(
            parsed,
            PostgresFrontendMessage::Other(PostgresMessage::new(vec![b'P', 0, 0, 0, 7, 1, 2, 3]))
        );
    }

    #[tokio::test]
    async fn test_proto_fe_message_parse_invalid_tag() {
        test_parse(&[b'1', 0, 0, 0, 7, 1, 2, 3]).await.unwrap_err();
    }
}
