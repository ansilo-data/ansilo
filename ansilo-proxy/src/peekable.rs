use std::{
    cmp,
    io::{self},
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

/// Connection to make a stream "peekable".
/// Eg we can look ahead at the incoming data without consuming it for future reads.
pub struct Peekable<S: AsyncRead + AsyncWrite> {
    pub(crate) inner: S,
    peeked: Vec<u8>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Peekable<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            peeked: vec![],
        }
    }

    /// Returns the underlying stream
    #[allow(unused)]
    pub fn inner(self) -> S {
        self.inner
    }

    /// Peeks ahead of the current read position
    /// This will read exactly the requested number of bytes
    /// or fail if the underlying stream ends prematurely.
    pub async fn peek(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let cur_peeked = self.peeked.len();
        if cur_peeked < buf.len() {
            self.peeked.resize(buf.len(), 0);
            self.inner
                .read_exact(&mut self.peeked[cur_peeked..])
                .await?;
        }

        buf.copy_from_slice(&self.peeked[..]);
        Ok(())
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for Peekable<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut read = 0;

        if !self.peeked.is_empty() {
            read += cmp::min(buf.remaining(), self.peeked.len());
            buf.put_slice(&self.peeked[..read]);
            self.peeked.drain(..read);
        }

        if buf.remaining() > 0 {
            match Pin::new(&mut self.inner).poll_read(cx, buf) {
                Poll::Ready(res) => Poll::Ready(res),
                Poll::Pending if read > 0 => Poll::Ready(Ok(())),
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Peekable<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;

    fn mock_peekable(data: Vec<u8>) -> Peekable<io::Cursor<Vec<u8>>> {
        Peekable::new(io::Cursor::new(data))
    }

    #[tokio::test]
    async fn test_read_empty() {
        let mut s = mock_peekable(vec![]);
        let mut buf = [0u8; 1024];

        assert_eq!(s.read(&mut buf).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_read_data() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 1024];

        assert_eq!(s.read(&mut buf[..2]).await.unwrap(), 2);
        assert_eq!(s.read(&mut buf[2..]).await.unwrap(), 3);
        assert_eq!(s.read(&mut buf[..]).await.unwrap(), 0);
        assert_eq!(&buf[..5], [1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_write_to_empty() {
        let mut s = mock_peekable(vec![]);

        s.write_all(&[1, 2, 3, 4, 5]).await.unwrap();

        assert_eq!(s.inner().into_inner(), vec![1, 2, 3, 4, 5])
    }

    #[tokio::test]
    async fn test_write_to_data() {
        let mut s = mock_peekable(vec![11, 12, 13, 14, 15, 16, 17, 18, 19]);

        s.write_all(&[1, 2, 3, 4, 5]).await.unwrap();

        assert_eq!(s.inner().into_inner(), vec![1, 2, 3, 4, 5, 16, 17, 18, 19])
    }

    #[tokio::test]
    async fn test_peek_empty() {
        let mut s = mock_peekable(vec![]);
        let mut buf = [0u8; 1];

        s.peek(&mut buf).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_peek_past_end() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 10];

        s.peek(&mut buf).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_peek_partial() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 3];

        s.peek(&mut buf).await.unwrap();

        assert_eq!(buf, [1, 2, 3]);
    }

    #[tokio::test]
    async fn test_peek_all() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 5];

        s.peek(&mut buf).await.unwrap();

        assert_eq!(buf, [1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_peek_partial_then_read() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 3];
        let mut buf = [0u8; 10];

        s.peek(&mut peek).await.unwrap();

        assert_eq!(peek, [1, 2, 3]);

        assert_eq!(s.read(&mut buf).await.unwrap(), 5);
        assert_eq!(buf, [1, 2, 3, 4, 5, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_peek_all_then_read() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 5];
        let mut buf = [0u8; 10];

        s.peek(&mut peek).await.unwrap();

        assert_eq!(peek, [1, 2, 3, 4, 5]);

        assert_eq!(s.read(&mut buf).await.unwrap(), 5);
        assert_eq!(buf, [1, 2, 3, 4, 5, 0, 0, 0, 0, 0]);
    }

    #[tokio::test]
    async fn test_peek_partial_then_read_partial() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 3];
        let mut buf = [0u8; 2];

        s.peek(&mut peek).await.unwrap();

        assert_eq!(peek, [1, 2, 3]);

        assert_eq!(s.read(&mut buf).await.unwrap(), 2);
        assert_eq!(buf, [1, 2]);
        assert_eq!(s.read(&mut buf).await.unwrap(), 2);
        assert_eq!(buf, [3, 4]);
        assert_eq!(s.read(&mut buf).await.unwrap(), 1);
        assert_eq!(buf, [5, 4]);
    }

    #[tokio::test]
    async fn test_peek_all_then_read_partial() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 5];
        let mut buf = [0u8; 2];

        s.peek(&mut peek).await.unwrap();

        assert_eq!(peek, [1, 2, 3, 4, 5]);

        assert_eq!(s.read(&mut buf).await.unwrap(), 2);
        assert_eq!(buf, [1, 2]);
        assert_eq!(s.read(&mut buf).await.unwrap(), 2);
        assert_eq!(buf, [3, 4]);
        assert_eq!(s.read(&mut buf).await.unwrap(), 1);
        assert_eq!(buf, [5, 4]);
    }

    #[tokio::test]
    async fn test_multiple_peeks() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 5];

        s.peek(&mut peek[..1]).await.unwrap();
        assert_eq!(peek, [1, 0, 0, 0, 0]);

        s.peek(&mut peek[..2]).await.unwrap();
        assert_eq!(peek, [1, 2, 0, 0, 0]);

        s.peek(&mut peek[..]).await.unwrap();
        assert_eq!(peek, [1, 2, 3, 4, 5]);
    }
}
