#[derive(Debug, Clone, Copy)]
pub enum HttpMode {
    Http1,
    Http2,
}

impl HttpMode {
    #[must_use]
    pub fn is_http1(&self) -> bool {
        matches!(self, Self::Http1)
    }

    #[must_use]
    pub fn is_http2(&self) -> bool {
        matches!(self, Self::Http2)
    }
}
