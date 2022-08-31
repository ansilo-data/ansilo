use ansilo_core::{config::PasswordUserConfig, err::Result};
use md5::{Digest, Md5};
use subtle::ConstantTimeEq;

/// Used for validating passwords
///
/// Current we support validating MD5-based hashes
/// but in future we want to move to SCRAM auth.
#[derive(Debug, Default)]
pub struct PasswordAuthProvider;

impl PasswordAuthProvider {
    /// Authenticates the supplied md5 password hash
    pub fn authenticate(
        &self,
        user: &PasswordUserConfig,
        md5_password_hash: &[u8],
    ) -> Result<bool> {
        let mut hasher = Md5::new();
        hasher.update(user.password.as_bytes());

        let expected = hasher.finalize().to_vec();

        let matches = expected.as_slice().ct_eq(md5_password_hash);

        Ok(matches.unwrap_u8() == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_auth_invalid() {
        let provider = PasswordAuthProvider::default();
        let user = PasswordUserConfig {
            password: "abc123".into(),
        };

        assert!(!provider.authenticate(&user, b"fgsdgfgfdgd").unwrap());
    }

    #[test]
    fn test_password_auth_valid() {
        let provider = PasswordAuthProvider::default();
        let user = PasswordUserConfig {
            password: "abc123".into(),
        };

        assert!(provider
            .authenticate(
                &user,
                &[233, 154, 24, 196, 40, 203, 56, 213, 242, 96, 133, 54, 120, 146, 46, 3]
            )
            .unwrap());
    }
}
