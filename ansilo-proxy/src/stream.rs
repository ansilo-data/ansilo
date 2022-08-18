use std::io::{self, Read, Write};

/// An IO stream
pub struct Stream<S: Read + Write>(pub S);

pub trait IOStream: Read + Write + Send {
    /// Returns a downcastable Any of the handler
    #[cfg(test)]
    fn as_any(&mut self) -> &mut dyn std::any::Any;
}

impl<S: Read + Write> Read for Stream<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl<S: Read + Write> Write for Stream<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<S: Read + Write + Send + 'static> IOStream for Stream<S> {
    #[cfg(test)]
    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
