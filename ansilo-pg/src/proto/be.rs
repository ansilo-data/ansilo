// @see https://www.postgresql.org/docs/current/protocol-message-formats.html

use std::{convert::TryInto, ffi::CString, io::Cursor};

use ansilo_core::err::{bail, ensure, Context, Error, Result};
use tokio::io::{AsyncRead, AsyncWrite};

use super::common::{CancelKey, PostgresMessage};

/// Postgres messages that are sent from the backend.
/// We only care about authentication, query and error messages, we treat the rest as opaque.
#[derive(Debug, PartialEq, Clone)]
pub enum PostgresBackendMessage {
    AuthenticationOk,
    AuthenticationMd5Password([u8; 4]),
    AuthenticationSasl(Vec<String>),
    AuthenticationSaslContinue(Vec<u8>),
    AuthenticationSaslFinal(Vec<u8>),
    AuthenticationCleartextPassword,
    ParameterStatus(String, String),
    ErrorResponse(Vec<(u8, String)>),
    ReadyForQuery(u8),
    BackendKeyData(CancelKey),
    Other(PostgresMessage),
}

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PostgresBackendMessageTag {
    Authentication = b'R',
    BackendKeyData = b'K',
    BindComplete = b'2',
    CloseComplete = b'3',
    CommandComplete = b'C',
    CopyInResponse = b'G',
    CopyOutResponse = b'H',
    CopyBothResponse = b'W',
    CopyData = b'd',
    CopyDone = b'c',
    DataRow = b'D',
    EmptyQueryResponse = b'I',
    ErrorResponse = b'E',
    FunctionCallResponse = b'V',
    NegotiateProtocolVersion = b'v',
    NoData = b'n',
    NoticeResponse = b'N',
    NotificationResponse = b'A',
    ParameterDescription = b't',
    ParameterStatus = b'S',
    ParseComplete = b'1',
    PortalSuspended = b's',
    ReadyForQuery = b'Z',
    RowDescription = b'T',
}

impl TryFrom<u8> for PostgresBackendMessageTag {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        Ok(match value {
            b'R' => Self::Authentication,
            b'K' => Self::BackendKeyData,
            b'2' => Self::BindComplete,
            b'3' => Self::CloseComplete,
            b'C' => Self::CommandComplete,
            b'G' => Self::CopyInResponse,
            b'H' => Self::CopyOutResponse,
            b'W' => Self::CopyBothResponse,
            b'd' => Self::CopyData,
            b'c' => Self::CopyDone,
            b'D' => Self::DataRow,
            b'I' => Self::EmptyQueryResponse,
            b'E' => Self::ErrorResponse,
            b'V' => Self::FunctionCallResponse,
            b'v' => Self::NegotiateProtocolVersion,
            b'n' => Self::NoData,
            b'N' => Self::NoticeResponse,
            b'A' => Self::NotificationResponse,
            b't' => Self::ParameterDescription,
            b'S' => Self::ParameterStatus,
            b'1' => Self::ParseComplete,
            b's' => Self::PortalSuspended,
            b'Z' => Self::ReadyForQuery,
            b'T' => Self::RowDescription,
            _ => bail!("Unknown backend message tag: {}", value),
        })
    }
}

impl PostgresBackendMessage {
    /// Reads a message from the postgres backend
    pub async fn read(stream: &mut (impl AsyncRead + Unpin)) -> Result<Self> {
        let message = PostgresMessage::read(stream).await?;

        Ok(match message.tag().unwrap().try_into()? {
            PostgresBackendMessageTag::ReadyForQuery => Self::ReadyForQuery(
                *message
                    .body()
                    .get(0)
                    .context("Malformed ReadyForQuery message from backend")?,
            ),
            PostgresBackendMessageTag::Authentication if message.body_length() >= 4 => {
                let auth_type = i32::from_be_bytes(message.body()[..4].try_into().unwrap());

                match auth_type {
                    0 => Self::AuthenticationOk,
                    3 => Self::AuthenticationCleartextPassword,
                    5 if message.body_length() == 8 => {
                        Self::AuthenticationMd5Password(message.body()[4..8].try_into().unwrap())
                    }
                    10 => {
                        let methods = message.body()[4..]
                            .split(|i| *i == 0)
                            .filter(|i| i.len() > 0)
                            .map(|i| {
                                String::from_utf8(i.to_vec())
                                    .context("Failed to parse sasl auth method")
                            })
                            .collect::<Result<Vec<_>>>()?;

                        Self::AuthenticationSasl(methods)
                    }
                    11 => Self::AuthenticationSaslContinue(message.body()[4..].to_vec()),
                    12 => Self::AuthenticationSaslFinal(message.body()[4..].to_vec()),
                    _ => Self::Other(message),
                }
            }
            PostgresBackendMessageTag::ParameterStatus => {
                let strings = message
                    .body()
                    .split(|i| *i == 0)
                    .take(2)
                    .map(|str| String::from_utf8_lossy(str).to_string())
                    .collect::<Vec<_>>();
                ensure!(
                    strings.len() == 2,
                    "Invalid number of strings in ParameterStatus"
                );

                Self::ParameterStatus(strings[0].clone(), strings[1].clone())
            }
            // @see https://www.postgresql.org/docs/current/protocol-error-fields.html
            PostgresBackendMessageTag::ErrorResponse => {
                let fields = message
                    .body()
                    .split(|i| *i == 0)
                    .filter(|g| g.len() > 0)
                    .map(|f| {
                        let key = f.first().cloned().unwrap();
                        let val = String::from_utf8_lossy(&f[1..]).to_string();
                        (key, val)
                    })
                    .collect();

                Self::ErrorResponse(fields)
            }
            PostgresBackendMessageTag::BackendKeyData => {
                ensure!(
                    message.body_length() == 8,
                    "Invalid backend key data length"
                );
                let pid = u32::from_be_bytes(message.body()[..4].try_into().unwrap());
                let key = u32::from_be_bytes(message.body()[4..8].try_into().unwrap());
                Self::BackendKeyData(CancelKey { pid, key })
            }
            _ => Self::Other(message),
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

    /// Converts the message into a postgres message that can
    /// be sent over the wire.
    pub fn serialise(self) -> Result<PostgresMessage> {
        use std::io::Write;

        fn write_i32(body: &mut Cursor<Vec<u8>>, val: i32) -> Result<()> {
            body.write_all(val.to_be_bytes().as_slice())?;
            Ok(())
        }

        Ok(match self {
            Self::Other(m) => m,
            Self::AuthenticationOk => {
                PostgresMessage::build(PostgresBackendMessageTag::Authentication as _, |body| {
                    write_i32(body, 0)?;
                    Ok(())
                })?
            }
            Self::AuthenticationCleartextPassword => {
                PostgresMessage::build(PostgresBackendMessageTag::Authentication as _, |body| {
                    write_i32(body, 3)?;
                    Ok(())
                })?
            }
            Self::AuthenticationMd5Password(salt) => {
                PostgresMessage::build(PostgresBackendMessageTag::Authentication as _, |body| {
                    write_i32(body, 5)?;
                    body.write_all(salt.as_slice())?;
                    Ok(())
                })?
            }
            Self::AuthenticationSasl(methods) => PostgresMessage::build(
                PostgresBackendMessageTag::Authentication as _,
                move |body| {
                    write_i32(body, 10)?;
                    for method in methods.into_iter() {
                        body.write_all(
                            CString::new(&*method)
                                .context("Cannot convert sasl method to cstring")?
                                .as_bytes_with_nul(),
                        )?;
                    }
                    Ok(())
                },
            )?,
            Self::AuthenticationSaslContinue(data) => {
                PostgresMessage::build(PostgresBackendMessageTag::Authentication as _, |body| {
                    write_i32(body, 11)?;
                    body.write_all(data.as_slice())?;
                    Ok(())
                })?
            }
            Self::AuthenticationSaslFinal(data) => {
                PostgresMessage::build(PostgresBackendMessageTag::Authentication as _, |body| {
                    write_i32(body, 12)?;
                    body.write_all(data.as_slice())?;
                    Ok(())
                })?
            }
            Self::ParameterStatus(key, value) => {
                PostgresMessage::build(PostgresBackendMessageTag::ParameterStatus as _, |body| {
                    body.write_all(
                        CString::new(key.as_bytes())
                            .context("Cannot convert parameter key to cstring")?
                            .as_bytes_with_nul(),
                    )?;
                    body.write_all(
                        CString::new(value.as_bytes())
                            .context("Cannot convert parameter value to cstring")?
                            .as_bytes_with_nul(),
                    )?;
                    Ok(())
                })?
            }
            Self::ReadyForQuery(status) => {
                PostgresMessage::build(PostgresBackendMessageTag::ReadyForQuery as _, |body| {
                    body.write_all(&[status])?;
                    Ok(())
                })?
            }
            Self::ErrorResponse(msg) => {
                PostgresMessage::build(PostgresBackendMessageTag::ErrorResponse as _, |body| {
                    // @see https://www.postgresql.org/docs/current/protocol-error-fields.html
                    // Strings must be null terminated
                    for (key, val) in msg.into_iter() {
                        body.write_all(&[key])?;
                        body.write_all(
                            CString::new(val.as_bytes())
                                .context("Cannot convert error field to cstring")?
                                .as_bytes_with_nul(),
                        )?;
                    }
                    body.write_all(&[0])?;

                    Ok(())
                })?
            }
            Self::BackendKeyData(key) => {
                PostgresMessage::build(PostgresBackendMessageTag::BackendKeyData as _, |body| {
                    body.write_all(&key.pid.to_be_bytes())?;
                    body.write_all(&key.key.to_be_bytes())?;

                    Ok(())
                })?
            }
        })
    }

    /// Gets the message tag if there is one
    pub fn tag(&self) -> Result<PostgresBackendMessageTag> {
        Ok(match self {
            Self::AuthenticationOk => PostgresBackendMessageTag::Authentication,
            Self::AuthenticationMd5Password(_) => PostgresBackendMessageTag::Authentication,
            Self::AuthenticationSasl(_) => PostgresBackendMessageTag::Authentication,
            Self::AuthenticationSaslContinue(_) => PostgresBackendMessageTag::Authentication,
            Self::AuthenticationSaslFinal(_) => PostgresBackendMessageTag::Authentication,
            Self::AuthenticationCleartextPassword => PostgresBackendMessageTag::Authentication,
            Self::ParameterStatus(_, _) => PostgresBackendMessageTag::ParameterStatus,
            Self::ErrorResponse(_) => PostgresBackendMessageTag::ErrorResponse,
            Self::ReadyForQuery(_) => PostgresBackendMessageTag::ReadyForQuery,
            Self::BackendKeyData(_) => PostgresBackendMessageTag::BackendKeyData,
            Self::Other(msg) => msg.tag().context("Untagged message")?.try_into()?,
        })
    }

    /// Creates a custom error response
    pub fn error_msg(msg: impl Into<String>) -> Self {
        Self::ErrorResponse(vec![
            (b'S', "ERROR".into()),
            (b'C', "XX000".into()),
            (b'M', msg.into()),
        ])
    }
}

#[cfg(test)]
mod tests {
    use tokio_test::io::Builder;

    use super::*;

    async fn parse(buf: &[u8]) -> Result<PostgresBackendMessage> {
        let mut stream = Builder::new().read(buf).build();
        PostgresBackendMessage::read(&mut stream).await
    }

    fn to_buff(msg: PostgresBackendMessage) -> Vec<u8> {
        msg.serialise().unwrap().into_raw()
    }

    #[tokio::test]
    async fn test_proto_be_write() {
        let msg = PostgresBackendMessage::Other(PostgresMessage::Tagged(vec![1, 2, 3]));

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
            to_buff(PostgresBackendMessage::ErrorResponse(vec![
                (b'S', "ERROR".into()),
                (b'C', "XX000".into()),
                (b'M', "MSG".into())
            ])),
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
            to_buff(PostgresBackendMessage::Other(PostgresMessage::Tagged(
                vec![1, 2, 3]
            ))),
            vec![1u8, 2, 3]
        )
    }

    #[test]
    fn test_proto_be_serialise_parameter_status() {
        assert_eq!(
            to_buff(PostgresBackendMessage::ParameterStatus(
                "key".into(),
                "value".into()
            )),
            vec![
                b'S', // tag
                0, 0, 0, 14, // len
                b'k', b'e', b'y', 0, // key
                b'v', b'a', b'l', b'u', b'e', 0 // value
            ]
        )
    }

    #[test]
    fn test_proto_be_serialise_ready_for_query() {
        assert_eq!(
            to_buff(PostgresBackendMessage::ReadyForQuery(b'I')),
            vec![
                b'Z', // tag
                0, 0, 0, 5,    // len
                b'I', // status
            ]
        )
    }

    #[test]
    fn test_proto_be_serialise_backend_key_data() {
        assert_eq!(
            to_buff(PostgresBackendMessage::BackendKeyData(CancelKey {
                pid: 123,
                key: 156
            })),
            vec![
                b'K', // tag
                0, 0, 0, 12, // len
                0, 0, 0, 123, // pid
                0, 0, 0, 156, // key
            ]
        )
    }

    #[tokio::test]
    async fn test_proto_be_read_ready_for_query() {
        let parsed = parse(&[b'Z', 0, 0, 0, 5, b'I']).await.unwrap();

        assert_eq!(parsed, PostgresBackendMessage::ReadyForQuery(b'I'));
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::ReadyForQuery
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_authentication_ok() {
        let parsed = parse(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]).await.unwrap();

        assert_eq!(parsed, PostgresBackendMessage::AuthenticationOk);
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::Authentication
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_authentication_clear_text() {
        let parsed = parse(&[b'R', 0, 0, 0, 8, 0, 0, 0, 3]).await.unwrap();

        assert_eq!(
            parsed,
            PostgresBackendMessage::AuthenticationCleartextPassword
        );
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::Authentication
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_authentication_md5() {
        let parsed = parse(&[b'R', 0, 0, 0, 12, 0, 0, 0, 5, 1, 2, 3, 4])
            .await
            .unwrap();

        assert_eq!(
            parsed,
            PostgresBackendMessage::AuthenticationMd5Password([1, 2, 3, 4])
        );
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::Authentication
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_authentication_sasl() {
        let parsed = parse(&[
            b'R', 0, 0, 0, 16, 0, 0, 0, 10, b'a', b'b', b'c', 0, b'1', b'2', b'3', 0,
        ])
        .await
        .unwrap();

        assert_eq!(
            parsed,
            PostgresBackendMessage::AuthenticationSasl(vec!["abc".into(), "123".into()])
        );
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::Authentication
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_authentication_sasl_continue() {
        let parsed = parse(&[b'R', 0, 0, 0, 11, 0, 0, 0, 11, 1, 2, 3])
            .await
            .unwrap();

        assert_eq!(
            parsed,
            PostgresBackendMessage::AuthenticationSaslContinue(vec![1, 2, 3])
        );
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::Authentication
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_authentication_sasl_final() {
        let parsed = parse(&[b'R', 0, 0, 0, 11, 0, 0, 0, 12, 1, 2, 3])
            .await
            .unwrap();

        assert_eq!(
            parsed,
            PostgresBackendMessage::AuthenticationSaslFinal(vec![1, 2, 3])
        );
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::Authentication
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_parameter_status() {
        let parsed = parse(&[
            b'S', 0, 0, 0, 14, b'e', b'n', b'c', b'o', 0, b'd', b'i', b'n', b'g', 0,
        ])
        .await
        .unwrap();

        assert_eq!(
            parsed,
            PostgresBackendMessage::ParameterStatus("enco".into(), "ding".into())
        );
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::ParameterStatus
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_error_response() {
        let parsed = parse(&[
            b'E', 0, 0, 0, 15, b'S', b'E', b'R', b'R', 0, b'M', b'm', b's', b'g', 0, 0,
        ])
        .await
        .unwrap();

        assert_eq!(
            parsed,
            PostgresBackendMessage::ErrorResponse(vec![(b'S', "ERR".into()), (b'M', "msg".into())])
        );
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::ErrorResponse
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_backend_key_data() {
        let parsed = parse(&[b'K', 0, 0, 0, 12, 0, 0, 1, 0, 0, 0, 0, 234])
            .await
            .unwrap();

        assert_eq!(
            parsed,
            PostgresBackendMessage::BackendKeyData(CancelKey { pid: 256, key: 234 })
        );
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::BackendKeyData
        );
    }

    #[tokio::test]
    async fn test_proto_be_read_other() {
        let parsed = parse(&[b'T', 0, 0, 0, 4]).await.unwrap();

        assert_eq!(
            parsed,
            PostgresBackendMessage::Other(PostgresMessage::Tagged(vec![b'T', 0, 0, 0, 4]))
        );
        assert_eq!(
            parsed.tag().unwrap(),
            PostgresBackendMessageTag::RowDescription
        );
    }
}
