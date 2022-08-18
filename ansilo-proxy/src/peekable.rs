use std::{
    cmp,
    io::{self, Read, Write},
};

/// Connection to make a stream "peekable".
/// Eg we can look ahead at the incoming data without consuming it for future reads.
pub struct Peekable<S: Read + Write> {
    pub(crate) inner: S,
    peeked: Vec<u8>,
}

impl<S: Read + Write> Peekable<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            peeked: vec![],
        }
    }

    /// Returns the underlying stream
    pub fn inner(self) -> S {
        self.inner
    }

    /// Peeks ahead of the current read position
    /// This will read exactly the requested number of bytes
    /// or fail if the underlying stream ends prematurely.
    pub fn peek(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let cur_peeked = self.peeked.len();
        if cur_peeked < buf.len() {
            self.peeked.resize(buf.len(), 0);
            self.inner.read_exact(&mut self.peeked[cur_peeked..])?;
        }

        buf.copy_from_slice(&self.peeked[..]);
        Ok(())
    }
}

impl<S: Read + Write> Read for Peekable<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read = 0;

        if !self.peeked.is_empty() {
            read += cmp::min(buf.len(), self.peeked.len());
            buf[..read].copy_from_slice(&self.peeked[..read]);
            self.peeked.drain(..read);
        }

        if read < buf.len() {
            read += self.inner.read(&mut buf[read..])?;
        }

        Ok(read)
    }
}

impl<S: Read + Write> Write for Peekable<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_peekable(data: Vec<u8>) -> Peekable<io::Cursor<Vec<u8>>> {
        Peekable::new(io::Cursor::new(data))
    }

    #[test]
    fn test_read_empty() {
        let mut s = mock_peekable(vec![]);
        let mut buf = [0u8; 1024];

        assert_eq!(s.read(&mut buf).unwrap(), 0);
    }

    #[test]
    fn test_read_data() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 1024];

        assert_eq!(s.read(&mut buf[..2]).unwrap(), 2);
        assert_eq!(s.read(&mut buf[2..]).unwrap(), 3);
        assert_eq!(s.read(&mut buf[..]).unwrap(), 0);
        assert_eq!(&buf[..5], [1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_write_to_empty() {
        let mut s = mock_peekable(vec![]);

        s.write_all(&[1, 2, 3, 4, 5]).unwrap();

        assert_eq!(s.inner().into_inner(), vec![1, 2, 3, 4, 5])
    }

    #[test]
    fn test_write_to_data() {
        let mut s = mock_peekable(vec![11, 12, 13, 14, 15, 16, 17, 18, 19]);

        s.write_all(&[1, 2, 3, 4, 5]).unwrap();

        assert_eq!(s.inner().into_inner(), vec![1, 2, 3, 4, 5, 16, 17, 18, 19])
    }

    #[test]
    fn test_peek_empty() {
        let mut s = mock_peekable(vec![]);
        let mut buf = [0u8; 1];

        s.peek(&mut buf).unwrap_err();
    }

    #[test]
    fn test_peek_past_end() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 10];

        s.peek(&mut buf).unwrap_err();
    }

    #[test]
    fn test_peek_partial() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 3];

        s.peek(&mut buf).unwrap();

        assert_eq!(buf, [1, 2, 3]);
    }

    #[test]
    fn test_peek_all() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut buf = [0u8; 5];

        s.peek(&mut buf).unwrap();

        assert_eq!(buf, [1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_peek_partial_then_read() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 3];
        let mut buf = [0u8; 10];

        s.peek(&mut peek).unwrap();

        assert_eq!(peek, [1, 2, 3]);

        assert_eq!(s.read(&mut buf).unwrap(), 5);
        assert_eq!(buf, [1, 2, 3, 4, 5, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_peek_all_then_read() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 5];
        let mut buf = [0u8; 10];

        s.peek(&mut peek).unwrap();

        assert_eq!(peek, [1, 2, 3, 4, 5]);

        assert_eq!(s.read(&mut buf).unwrap(), 5);
        assert_eq!(buf, [1, 2, 3, 4, 5, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn test_peek_partial_then_read_partial() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 3];
        let mut buf = [0u8; 2];

        s.peek(&mut peek).unwrap();

        assert_eq!(peek, [1, 2, 3]);

        assert_eq!(s.read(&mut buf).unwrap(), 2);
        assert_eq!(buf, [1, 2]);
        assert_eq!(s.read(&mut buf).unwrap(), 2);
        assert_eq!(buf, [3, 4]);
        assert_eq!(s.read(&mut buf).unwrap(), 1);
        assert_eq!(buf, [5, 4]);
    }

    #[test]
    fn test_peek_all_then_read_partial() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 5];
        let mut buf = [0u8; 2];

        s.peek(&mut peek).unwrap();

        assert_eq!(peek, [1, 2, 3, 4, 5]);

        assert_eq!(s.read(&mut buf).unwrap(), 2);
        assert_eq!(buf, [1, 2]);
        assert_eq!(s.read(&mut buf).unwrap(), 2);
        assert_eq!(buf, [3, 4]);
        assert_eq!(s.read(&mut buf).unwrap(), 1);
        assert_eq!(buf, [5, 4]);
    }

    #[test]
    fn test_multiple_peeks() {
        let mut s = mock_peekable(vec![1, 2, 3, 4, 5]);
        let mut peek = [0u8; 5];

        s.peek(&mut peek[..1]).unwrap();
        assert_eq!(peek, [1, 0, 0, 0, 0]);

        s.peek(&mut peek[..2]).unwrap();
        assert_eq!(peek, [1, 2, 0, 0, 0]);

        s.peek(&mut peek[..]).unwrap();
        assert_eq!(peek, [1, 2, 3, 4, 5]);
    }
}
