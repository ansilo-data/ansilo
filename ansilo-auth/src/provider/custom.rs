use ansilo_core::{config::CustomAuthProviderConfig, err::Result};

pub struct CustomAuthProvider {
    _conf: &'static CustomAuthProviderConfig,
}

impl CustomAuthProvider {
    pub fn new(conf: &'static CustomAuthProviderConfig) -> Result<Self> {
        Ok(Self { _conf: conf })
    }
}
