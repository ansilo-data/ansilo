use ansilo_core::{config::SamlAuthProviderConfig, err::Result};

pub struct SamlAuthProvider {
    _conf: &'static SamlAuthProviderConfig,
}

impl SamlAuthProvider {
    pub fn new(conf: &'static SamlAuthProviderConfig) -> Result<Self> {
        Ok(Self { _conf: conf })
    }
}
