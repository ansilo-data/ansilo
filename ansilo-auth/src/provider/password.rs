use ansilo_core::{
    config::PasswordUserConfig,
    err::{bail, Result},
};
use md5::{Digest, Md5};
use subtle::ConstantTimeEq;

use crate::ctx::PasswordAuthContext;

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
        salt: &[u8],
        md5_password_hash: &[u8],
    ) -> Result<PasswordAuthContext> {
        let mut hasher = Md5::new();
        hasher.update(user.password.as_bytes());
        hasher.update(salt);

        let expected = hasher.finalize().to_vec();

        let matches = expected.as_slice().ct_eq(md5_password_hash);

        if matches.unwrap_u8() != 1 {
            bail!("Incorrect password")
        }

        Ok(PasswordAuthContext::default())
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

        assert!(provider
            .authenticate(&user, b"fgsdgfgfdgd", &[1, 2, 3])
            .is_err());
    }

    #[test]
    fn test_password_auth_valid() {
        let provider = PasswordAuthProvider::default();
        let user = PasswordUserConfig {
            password: "abc123".into(),
        };

        assert!(
            provider
                .authenticate(
                    &user,
                    &[1, 2, 3],
                    // echo "$(echo -n "abc123" | xxd -p)010203" | xxd -r -p | md5sum | xxd -r -p | od -tu1
                    &[98, 206, 227, 198, 78, 191, 205, 14, 44, 113, 220, 206, 231, 72, 227, 210]
                )
                .unwrap()
                == PasswordAuthContext::default()
        );
    }
}
