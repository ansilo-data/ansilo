// @see https://www.postgresql.org/docs/current/protocol-message-formats.html

use std::io::{self, Write};

use ansilo_core::err::{ensure, Context, Result};
use tokio::io::{AsyncRead, AsyncReadExt};

/// A generic postgres message
#[derive(Debug, Clone, PartialEq)]
pub enum PostgresMessage {
    /// The message payload
    Tagged(Vec<u8>),
    Untagged(Vec<u8>),
}

impl PostgresMessage {
    /// Reads a postgres message from the supplied stream
    pub async fn read(stream: &mut (impl AsyncRead + Unpin)) -> Result<Self> {
        let tag = stream
            .read_u8()
            .await
            .context("Failed to read postgres message tag")?;

        let len: i32 = stream
            .read_i32()
            .await
            .context("Failed to read postgres message length")?;

        // Message length includes itself
        ensure!(len >= 4, "Invalid message length");
        let full_len = len.checked_add(1).context("Invalid message length")?;

        // Reconstruct the entire message into a vec
        let mut buff = vec![0u8; full_len as _];
        buff[0] = tag;
        buff[1..=4].copy_from_slice(len.to_be_bytes().as_slice());
        stream.read_exact(&mut buff[5..]).await?;

        Ok(Self::Tagged(buff))
    }

    /// Reads an untagged postgres message from the supplied stream
    pub async fn read_untagged(stream: &mut (impl AsyncRead + Unpin)) -> Result<Self> {
        let len: i32 = stream
            .read_i32()
            .await
            .context("Failed to read postgres message length")?;

        // Message length includes itself
        ensure!(len >= 4, "Invalid message length");

        // Reconstruct the entire message into a vec
        let mut buff = vec![0u8; len as _];
        buff[0..=3].copy_from_slice(len.to_be_bytes().as_slice());
        stream.read_exact(&mut buff[4..]).await?;

        Ok(Self::Untagged(buff))
    }

    /// Builds a new postgres message from the supplied tag and calls f()
    /// to write the body
    pub(crate) fn build(
        tag: u8,
        body: impl FnOnce(&mut io::Cursor<Vec<u8>>) -> Result<()>,
    ) -> Result<Self> {
        let mut buff = io::Cursor::new(vec![tag, 0, 0, 0, 0]);
        buff.set_position(5);

        body(&mut buff).context("Failed to write postgres message body")?;
        buff.flush().context("Failed to flush buffer")?;

        let mut buff = buff.into_inner();
        // Calculate message length excluding tag
        let len = i32::try_from(buff.len() - 1)
            .context("Body is too large to write to postgres message")?;

        buff[1..=4].copy_from_slice(len.to_be_bytes().as_slice());

        Ok(Self::Tagged(buff))
    }

    /// Builds a new untagged postgres message from the supplied tag and calls f()
    /// to write the body
    pub(crate) fn build_untagged(
        body: impl FnOnce(&mut io::Cursor<Vec<u8>>) -> Result<()>,
    ) -> Result<Self> {
        let mut buff = io::Cursor::new(vec![0, 0, 0, 0]);
        buff.set_position(4);

        body(&mut buff).context("Failed to write postgres message body")?;
        buff.flush().context("Failed to flush buffer")?;

        let mut buff = buff.into_inner();
        let len = i32::try_from(buff.len())
            .context("Body is too large to write to postgres message")?;

        buff[0..=3].copy_from_slice(len.to_be_bytes().as_slice());

        Ok(Self::Untagged(buff))
    }

    /// Gets the raw message as a slice
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Tagged(b) => b.as_slice(),
            Self::Untagged(b) => b.as_slice(),
        }
    }

    /// Gets the postgres message tag
    pub fn tag(&self) -> u8 {
        match self {
            Self::Tagged(b) => b[0],
            Self::Untagged(_) => panic!("Message is not tagged"),
        }
    }

    /// Gets the postgres message length unchanged from the original message
    /// This includes the length of the body + length u32 but not the tag.
    pub fn raw_length(&self) -> i32 {
        i32::from_be_bytes(
            match self {
                Self::Tagged(b) => &b[1..=4],
                Self::Untagged(b) => &b[0..=3],
            }
            .try_into()
            .unwrap(),
        )
    }

    /// Gets the postgres message body length
    pub fn body_length(&self) -> i32 {
        self.raw_length() - 4
    }

    /// Gets the message body as a slice
    pub fn body(&self) -> &[u8] {
        match self {
            Self::Tagged(b) => &b[5..],
            Self::Untagged(b) => &b[4..],
        }
    }

    /// Returns the underlying message buffer
    pub fn into_raw(self) -> Vec<u8> {
        match self {
            Self::Tagged(b) => b,
            Self::Untagged(b) => b,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use ansilo_core::err::bail;
    use tokio_test::io::Builder;

    use super::*;

    async fn test_parse(buf: &[u8]) -> Result<PostgresMessage> {
        let mut stream = Builder::new().read(buf).build();
        PostgresMessage::read(&mut stream).await
    }

    async fn test_parse_untagged(buf: &[u8]) -> Result<PostgresMessage> {
        let mut stream = Builder::new().read(buf).build();
        PostgresMessage::read_untagged(&mut stream).await
    }

    #[tokio::test]
    async fn test_proto_common_message_parse_empty() {
        test_parse(&[]).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_proto_common_message_parse_invalid_length() {
        test_parse(&[b'A']).await.unwrap_err();
        test_parse(&[b'A', 1]).await.unwrap_err();
        test_parse(&[b'A', 1, 1]).await.unwrap_err();
        test_parse(&[b'A', 1, 1, 1]).await.unwrap_err();
        // message length cannt be < 4
        test_parse(&[b'A', 0, 0, 0, 3]).await.unwrap_err();
        // message length cannot overflow u32
        test_parse(&[b'A', 255, 255, 255, 255]).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_proto_common_message_parse_length_beyond_eof() {
        test_parse(&[b'A', 0, 0, 0, 8, 1, 2, 3]).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_proto_common_message_parse_valid_empty_body() {
        let parsed = test_parse(&[b'A', 0, 0, 0, 4]).await.unwrap();

        assert_eq!(parsed, PostgresMessage::Tagged(vec![b'A', 0, 0, 0, 4]));

        assert_eq!(parsed.tag(), b'A');
        assert_eq!(parsed.raw_length(), 4);
        assert_eq!(parsed.body_length(), 0);
        assert_eq!(parsed.as_slice(), &[b'A', 0, 0, 0, 4]);
        assert_eq!(parsed.body(), &[0u8; 0]);
    }

    #[tokio::test]
    async fn test_proto_common_message_parse_valid_with_body() {
        let parsed = test_parse(&[b'A', 0, 0, 0, 7, 1, 2, 3]).await.unwrap();

        assert_eq!(
            parsed,
            PostgresMessage::Tagged(vec![b'A', 0, 0, 0, 7, 1, 2, 3])
        );

        assert_eq!(parsed.tag(), b'A');
        assert_eq!(parsed.raw_length(), 7);
        assert_eq!(parsed.body_length(), 3);
        assert_eq!(parsed.as_slice(), &[b'A', 0, 0, 0, 7, 1, 2, 3]);
        assert_eq!(parsed.body(), &[1, 2, 3]);
    }

    #[tokio::test]
    async fn test_proto_common_message_parse_untagged_invalid_length() {
        test_parse_untagged(&[]).await.unwrap_err();
        test_parse_untagged(&[1]).await.unwrap_err();
        test_parse_untagged(&[1, 1]).await.unwrap_err();
        test_parse_untagged(&[1, 1, 1]).await.unwrap_err();
        // message length cannot be < 4
        test_parse_untagged(&[0, 0, 0, 3]).await.unwrap_err();
        // message length cannot overflow u32
        test_parse_untagged(&[255, 255, 255, 255])
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_proto_common_message_parse_untagged_length_beyond_eof() {
        test_parse_untagged(&[0, 0, 0, 8, 1, 2, 3])
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn test_proto_common_message_parse_untagged_valid_empty_body() {
        let parsed = test_parse_untagged(&[0, 0, 0, 4]).await.unwrap();

        assert_eq!(parsed, PostgresMessage::Untagged(vec![0, 0, 0, 4]));

        assert_eq!(parsed.raw_length(), 4);
        assert_eq!(parsed.body_length(), 0);
        assert_eq!(parsed.as_slice(), &[0, 0, 0, 4]);
        assert_eq!(parsed.body(), &[0u8; 0]);
    }

    #[tokio::test]
    async fn test_proto_common_message_parse_untagged_valid_with_body() {
        let parsed = test_parse_untagged(&[0, 0, 0, 7, 1, 2, 3]).await.unwrap();

        assert_eq!(parsed, PostgresMessage::Untagged(vec![0, 0, 0, 7, 1, 2, 3]));

        assert_eq!(parsed.raw_length(), 7);
        assert_eq!(parsed.body_length(), 3);
        assert_eq!(parsed.as_slice(), &[0, 0, 0, 7, 1, 2, 3]);
        assert_eq!(parsed.body(), &[1, 2, 3]);
    }

    #[test]
    fn test_proto_common_message_build_empty() {
        let built = PostgresMessage::build(b'a', |_| Ok(())).unwrap();

        assert_eq!(built, PostgresMessage::Tagged(vec![b'a', 0, 0, 0, 4]));
    }

    #[test]
    fn test_proto_common_message_build_with_body() {
        let built = PostgresMessage::build(b'B', |body| {
            body.write_all(&[1, 2, 3]).unwrap();
            Ok(())
        })
        .unwrap();

        assert_eq!(
            built,
            PostgresMessage::Tagged(vec![b'B', 0, 0, 0, 7, 1, 2, 3])
        );
    }

    #[test]
    fn test_proto_common_message_build_error() {
        PostgresMessage::build(b'a', |_| bail!("Error")).unwrap_err();
    }

    #[test]
    fn test_proto_common_message_build_untagged_empty() {
        let built = PostgresMessage::build_untagged(|_| Ok(())).unwrap();

        assert_eq!(built, PostgresMessage::Untagged(vec![0, 0, 0, 4]));
    }

    #[test]
    fn test_proto_common_message_build_untagged_with_body() {
        let built = PostgresMessage::build_untagged(|body| {
            body.write_all(&[1, 2, 3]).unwrap();
            Ok(())
        })
        .unwrap();

        assert_eq!(built, PostgresMessage::Untagged(vec![0, 0, 0, 7, 1, 2, 3]));
    }

    #[test]
    fn test_proto_common_message_build_untagged_error() {
        PostgresMessage::build_untagged(|_| bail!("Error")).unwrap_err();
    }
}
