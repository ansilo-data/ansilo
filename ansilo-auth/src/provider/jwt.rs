use ansilo_core::config::JwtAuthProviderConfig;



/// Used for validating JWT tokens.
#[derive(Debug)]
pub struct JwtAuthPorvider {
    /// Provider config
    conf: JwtAuthProviderConfig
}

// impl JwtAuthPorvider {
//     /// Authenticates the supplied md5 password hash
//     pub fn authenticate(&self, user: &PasswordUserConfig, md5_password_hash: &str) -> Result<bool> {
//         let mut hasher = Md5::new();
//         hasher.update(user.password.as_bytes());

//         let expected = hasher.finalize();

//         let matches = expected.ct_eq(md5_password_hash.as_bytes());

//         Ok(matches.unwrap_u8() == 1)
//     }
// }