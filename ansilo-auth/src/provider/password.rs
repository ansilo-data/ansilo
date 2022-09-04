use ansilo_core::{
    config::PasswordUserConfig,
    err::{bail, Result}, auth::PasswordAuthContext,
};
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
        username: &str,
        salt: &[u8],
        md5_password_hash: &[u8],
    ) -> Result<PasswordAuthContext> {
        // Stage 1 is md5(password + username)
        let mut hasher = Md5::new();
        hasher.update(user.password.as_bytes());
        hasher.update(username);
        let stage1 = hasher.finalize().to_vec();

        // Stage 2 is md5(hex(stage1) + salt)
        let stage1 = hex::encode(stage1);
        let mut hasher = Md5::new();
        hasher.update(stage1.as_bytes());
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
            .authenticate(&user, "user", b"fgsdgfgfdgd", &[1, 2, 3])
            .is_err());
    }

    #[test]
    fn test_password_auth_valid() {
        let provider = PasswordAuthProvider::default();
        let username = "user";
        let user = PasswordUserConfig {
            password: "abc123".into(),
        };

        assert!(
            provider
                .authenticate(
                    &user,
                    username,
                    &[b'f', b'o', b'o'],
                    // echo -n "$(echo -n "abc123user" | md5sum | cut -d' ' -f1)foo" | md5sum | cut -d' ' -f1 | xxd -r -p | od -tu1
                    &[95, 186, 186, 149, 71, 220, 39, 145, 178, 249, 92, 58, 85, 30, 103, 164]
                )
                .unwrap()
                == PasswordAuthContext::default()
        );
    }
}
