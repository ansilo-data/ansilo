// @see https://www.postgresql.org/docs/current/protocol-message-formats.html

use std::{
    ffi::CString,
    io::{Cursor, Write},
};

use ansilo_core::err::{Context, Result};
use tokio::io::AsyncWrite;

use super::common::PostgresMessage;

/// Postgres messages that are sent from the backend.
/// We only care about authentication and error messages, rest we treat as opaque.
#[derive(Debug, PartialEq, Clone)]
pub enum PostgresBackendMessage {
    AuthenticationOk,
    AuthenticationMd5Password([u8; 4]),
    AuthenticationSasl(Vec<String>),
    AuthenticationSaslContinue(Vec<u8>),
    AuthenticationSaslFinal(Vec<u8>),
    AuthenticationCleartextPassword,
    ErrorResponse(String),
    Other(PostgresMessage),
}

impl PostgresBackendMessage {
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

    /// Converts the message into a postgres message that can
    /// be sent over the wire.
    pub fn serialise(self) -> Result<PostgresMessage> {
        fn write_i32(body: &mut Cursor<Vec<u8>>, val: i32) -> Result<()> {
            body.write_all(val.to_be_bytes().as_slice())?;
            Ok(())
        }

        Ok(match self {
            Self::Other(m) => m,
            Self::AuthenticationOk => PostgresMessage::build(b'R', |body| {
                write_i32(body, 0)?;
                Ok(())
            })?,
            Self::AuthenticationCleartextPassword => PostgresMessage::build(b'R', |body| {
                write_i32(body, 3)?;
                Ok(())
            })?,
            Self::AuthenticationMd5Password(salt) => PostgresMessage::build(b'R', |body| {
                write_i32(body, 5)?;
                body.write_all(salt.as_slice())?;
                Ok(())
            })?,
            Self::AuthenticationSasl(methods) => PostgresMessage::build(b'R', move |body| {
                write_i32(body, 10)?;
                for method in methods.into_iter() {
                    body.write_all(
                        CString::new(&*method)
                            .context("Cannot convert sasl method to cstring")?
                            .as_bytes_with_nul(),
                    )?;
                }
                Ok(())
            })?,
            Self::AuthenticationSaslContinue(data) => PostgresMessage::build(b'R', |body| {
                write_i32(body, 11)?;
                body.write_all(data.as_slice())?;
                Ok(())
            })?,
            Self::AuthenticationSaslFinal(data) => PostgresMessage::build(b'R', |body| {
                write_i32(body, 12)?;
                body.write_all(data.as_slice())?;
                Ok(())
            })?,
            Self::ErrorResponse(msg) => {
                PostgresMessage::build(b'E', |body| {
                    // @see https://www.postgresql.org/docs/current/protocol-error-fields.html
                    // Strings must be null terminated
                    body.write_all(&[b'S'])?;
                    body.write_all(b"ERROR\0")?;
                    body.write_all(&[b'C'])?;
                    body.write_all(b"XX000\0")?;
                    body.write_all(&[b'M'])?;
                    body.write_all(
                        CString::new(&*msg)
                            .context("Cannot convert error message to cstring")?
                            .as_bytes_with_nul(),
                    )?;
                    body.write_all(&[0])?;

                    Ok(())
                })?
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio_test::io::Builder;

    use super::*;

    fn to_buff(msg: PostgresBackendMessage) -> Vec<u8> {
        msg.serialise().unwrap().into_raw()
    }

    #[tokio::test]
    async fn test_proto_be_write() {
        let msg = PostgresBackendMessage::Other(PostgresMessage::new(vec![1, 2, 3]));

        let mut stream = Builder::new().write(&[1, 2, 3]).build();

        msg.write(&mut stream).await.unwrap();
    }

    #[test]
    fn test_proto_be_serialise_authentication_ok() {
        assert_eq!(
            to_buff(PostgresBackendMessage::AuthenticationOk),
            vec![
                b'R', // tag
                0, 0, 0, 8, // len
                0, 0, 0, 0, // subtype
            ]
        )
    }

    #[test]
    fn test_proto_be_serialise_authentication_md5_password() {
        assert_eq!(
            to_buff(PostgresBackendMessage::AuthenticationMd5Password([
                1, 2, 3, 4
            ])),
            vec![
                b'R', // tag
                0, 0, 0, 12, // len
                0, 0, 0, 5, // subtype
                1, 2, 3, 4 // salt
            ]
        )
    }

    #[test]
    fn test_proto_be_serialise_authentication_sasl() {
        assert_eq!(
            to_buff(PostgresBackendMessage::AuthenticationSasl(vec![
                "first".into(),
                "second".into()
            ])),
            vec![
                b'R', // tag
                0, 0, 0, 21, // len
                0, 0, 0, 10, // subtype
                b'f', b'i', b'r', b's', b't', 0, // first method
                b's', b'e', b'c', b'o', b'n', b'd', 0 // second method
            ]
        )
    }

    #[test]
    fn test_proto_be_serialise_authentication_sasl_continue() {
        assert_eq!(
            to_buff(PostgresBackendMessage::AuthenticationSaslContinue(vec![
                1, 2, 3, 4
            ])),
            vec![
                b'R', // tag
                0, 0, 0, 12, // len
                0, 0, 0, 11, // subtype
                1, 2, 3, 4 // data
            ]
        )
    }

    #[test]
    fn test_proto_be_serialise_authentication_sasl_final() {
        assert_eq!(
            to_buff(PostgresBackendMessage::AuthenticationSaslFinal(vec![
                1, 2, 3, 4, 5
            ])),
            vec![
                b'R', // tag
                0, 0, 0, 13, // len
                0, 0, 0, 12, // subtype
                1, 2, 3, 4, 5 // data
            ]
        )
    }

    #[test]
    fn test_proto_be_serialise_authentication_clear_text_password() {
        assert_eq!(
            to_buff(PostgresBackendMessage::AuthenticationCleartextPassword),
            vec![
                b'R', // tag
                0, 0, 0, 8, // len
                0, 0, 0, 3, // subtype
            ]
        )
    }

    #[test]
    fn test_proto_be_serialise_error_response() {
        assert_eq!(
            to_buff(PostgresBackendMessage::ErrorResponse("MSG".into())),
            vec![
                b'E', // tag
                0, 0, 0, 24, // len
                b'S', b'E', b'R', b'R', b'O', b'R', 0, // severity field
                b'C', b'X', b'X', b'0', b'0', b'0', 0, // sqlstate field
                b'M', b'M', b'S', b'G', 0, // message field
                0, // terminator
            ]
        )
    }

    #[test]
    fn test_proto_be_serialise_other() {
        assert_eq!(
            to_buff(PostgresBackendMessage::Other(PostgresMessage::new(vec![
                1, 2, 3
            ]))),
            vec![1u8, 2, 3]
        )
    }
}
