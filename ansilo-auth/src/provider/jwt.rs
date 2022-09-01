use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use ansilo_core::{
    config::{JwtAuthProviderConfig, JwtUserConfig},
    err::{bail, ensure, Context, Error, Result},
};
use ansilo_logging::{info, warn};
use jsonwebkey::KeyUse;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, TokenData, Validation};
use serde::Deserialize;

use crate::{common::validate_check, ctx::JwtAuthContext};

/// Used for validating JWT tokens.
pub struct JwtAuthPorvider {
    /// Provider config
    _conf: JwtAuthProviderConfig,
    /// Shared state
    state: Arc<Mutex<State>>,
}

/// Shared state which contains the JWT decoding key
struct State {
    /// Decoding keys used to validate JWTs
    verification_keys: Vec<VerificationKey>,
    /// Whether the provider has been dropped
    dropped: bool,
}

struct VerificationKey {
    /// Option key id
    kid: Option<String>,
    /// Supported algorithms
    algs: Vec<Algorithm>,
    /// Decoding key used to validate JWTs
    key: DecodingKey,
}

impl JwtAuthPorvider {
    pub fn new(conf: JwtAuthProviderConfig) -> Result<Self> {
        ensure!(
            conf.jwk.is_some() || conf.rsa_public_key.is_some() || conf.ec_public_key.is_some()  || conf.ed_public_key.is_some(),
            "Must specify either 'jwk', 'rsa_public_key', 'ec_public_key', or 'ed_public_key' options for jwt auth provider"
        );

        let keys = Self::retrieve_decoding_keys(&conf)?;
        let state = Arc::new(Mutex::new(State::new(keys)));

        Self::periodically_update_keys(conf.clone(), Arc::clone(&state));

        Ok(Self { _conf: conf, state })
    }

    /// Authenticates the supplied JWT token
    pub fn authenticate(&self, user: &JwtUserConfig, jwt: &str) -> Result<JwtAuthContext> {
        // Retrieve of current state of JWT decoding keys
        // Important: we cannot panic in this block or we will poison the mutex
        // and all future requests will fail
        let state = self
            .state
            .lock()
            .map_err(|_| Error::msg("Failed to lock decoding keys"))?;
        let keys = &state.verification_keys;

        let header = decode_header(jwt).context("Failed to decode JWT header")?;

        // Find matching decoding key
        let key = if keys.len() > 1 {
            let kid = match &header.kid {
                Some(id) => id,
                None => bail!("Token must have a 'kid' header field"),
            };

            keys.into_iter()
                .find(|k| k.kid.as_ref() == Some(kid))
                .with_context(|| {
                    format!("Failed to find matching verification key for kid {}", kid)
                })?
        } else {
            keys.first().context("No verification key found")?
        };

        // Authenticate token
        ensure!(
            key.algs.contains(&header.alg),
            "Invalid 'alg' in JWT header found"
        );
        let validation = Validation::new(header.alg.clone());
        let decoded_token: TokenData<HashMap<String, serde_json::Value>> =
            decode(jwt, &key.key, &validation).context("Failed to authenticate JWT")?;

        // Verify claims
        for (claim, check) in user.claims.iter() {
            let actual = decoded_token.claims.get(claim);
            validate_check("claim", claim, actual, check)?;
        }

        // Token verified and passes checks
        Ok(JwtAuthContext {
            raw_token: jwt.to_string(),
            header,
            claims: decoded_token.claims,
        })
    }

    /// Retrieves a new decoding key
    fn retrieve_decoding_keys(conf: &JwtAuthProviderConfig) -> Result<Vec<VerificationKey>> {
        if let Some(jwk_url) = conf.jwk.as_ref() {
            #[derive(Deserialize)]
            struct JwkSet {
                jwk: Vec<jsonwebkey::JsonWebKey>,
            }

            info!("Retrieving JWK's from {}", jwk_url);
            let data = ansilo_util_url::get(jwk_url).context("Failed to get JWK")?;
            let set: JwkSet =
                serde_json::from_slice(data.as_slice()).context("Failed to parse JWK JSON")?;

            ensure!(set.jwk.len() > 0, "Found empty JWK set");

            return set
                .jwk
                .into_iter()
                .filter(|j| j.key_use.is_none() || j.key_use.unwrap() == KeyUse::Signing)
                .map(|j| {
                    Ok(VerificationKey {
                        kid: j.key_id,
                        algs: vec![j.algorithm.context("JWK 'alg' field is required")?.into()],
                        key: j.key.to_decoding_key(),
                    })
                })
                .collect();
        };

        if let Some(key) = conf.rsa_public_key.as_ref() {
            info!("Retrieving RSA public key from {}", key);
            let key = ansilo_util_url::get(key).context("Failed to get RSA public key")?;

            return Ok(vec![VerificationKey {
                kid: None,
                algs: vec![Algorithm::RS256, Algorithm::RS384, Algorithm::RS512],
                key: DecodingKey::from_rsa_pem(key.as_slice())
                    .context("Failed to decode RSA public key PEM file")?,
            }]);
        }

        if let Some(key) = conf.ec_public_key.as_ref() {
            info!("Retrieving EC public key from {}", key);
            let key = ansilo_util_url::get(key).context("Failed to get EC public key")?;

            return Ok(vec![VerificationKey {
                kid: None,
                algs: vec![Algorithm::ES256, Algorithm::ES384],
                key: DecodingKey::from_ec_pem(key.as_slice())
                    .context("Failed to decode EC public key PEM file")?,
            }]);
        }

        if let Some(key) = conf.ed_public_key.as_ref() {
            info!("Retrieving ED public key from {}", key);
            let key = ansilo_util_url::get(key).context("Failed to get ED public key")?;

            return Ok(vec![VerificationKey {
                kid: None,
                algs: vec![Algorithm::EdDSA],
                key: DecodingKey::from_ed_pem(key.as_slice())
                    .context("Failed to decode ED public key PEM file")?,
            }]);
        }

        unreachable!()
    }

    fn periodically_update_keys(conf: JwtAuthProviderConfig, state: Arc<Mutex<State>>) {
        thread::spawn(move || {
            loop {
                // TODO[low]: configurable update interval
                thread::sleep(Duration::from_secs(3600));

                let dropped = {
                    let state = state.lock().unwrap();
                    state.dropped
                };

                if dropped {
                    break;
                }

                info!("Periodic updated of JWT verification keys");
                let keys = match Self::retrieve_decoding_keys(&conf) {
                    Ok(keys) => keys,
                    Err(err) => {
                        warn!("Failed to update JWT verification keys: {:?}", err);
                        continue;
                    }
                };

                {
                    let mut state = state.lock().unwrap();
                    state.verification_keys = keys;
                }
            }
        });
    }
}

impl Drop for JwtAuthPorvider {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.dropped = true;
        }
    }
}

impl State {
    fn new(verification_keys: Vec<VerificationKey>) -> Self {
        Self {
            verification_keys,
            dropped: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        time::{SystemTime, UNIX_EPOCH},
    };

    use ansilo_core::config::TokenClaimCheck;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::Value;
    use tempfile::NamedTempFile;

    use super::*;

    const RSA_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEA0RNM7roNOPOvflr5JfMsvHSqlbNo0zorU6GSq1kzj5aHPzom
q6+IOpEEPYVqz+uGOkI4z9uUrc5e+OnN0y6s/3IKzC5rvTn9mu34jQGFG9wvJn4j
nUhTXi0onQpNQeQjT/bNcYaE1QNu/xZWloaGL2h7lMPkgKCUrMvMhfgmHjYl1wyU
pSzmyQBSHvRQwpit06RE2zIpaDz/BexIzedQI8rNFqwb6JCwyzqLHvFfi+Z9bIsk
5hypOOJqmn+t5RL3RPO6FSAlS4Gmo4D6TYguDKctQgxrjkNFDntkaZ5CkhZBaEFQ
MxodcPRAFpey7c4Ps0SHtYEiIvt5KunEQdxWVQIDAQABAoIBAAbHdm4jMDyRgjY5
oux9Fw1BRyq1d4ep0i/TBFtz9/0G53nAW/KjLWqgux93jQAS5fZLoWXz+70q/N9b
TWY0lD1wKDN4qdun3rZAUxfXXcaKfCjmXCdEaheapT96Twj86bw1B3JP50y04Mt6
oPIeiIlO/PrU4zrcehWkQgxyAGJoVg7mvbuKtwc/5Lhv0o/9NohgzHswNu4V/TK6
ahujkxuJ3FILaatEWvpU/OyE+Rj+JbLI59IR7dPNdNJFmW5UbQj55NwbvHpioC11
j7eQu73knCQ12Z5ygdzfNzpQ236Lf8ydXgR7GnrNT1Fw7Jfxfr6bE3qUK1Ntza04
8314nn0CgYEA6M7x3EupeldUF7of26P7qfKbDubVx4DWcFWUmNCoxj4T0Zr2m+sc
Hvr2SC8lBg+kl7IEU2GuUio73XbXiGhVfDHwz62LTPW5owgFqb+n2LPfKdOnN5c4
E+ms8lEFxyQCIaL6x3zYMtKnDw1JC3dm7onGGXMAnNoeL5r3X67CI3sCgYEA5ece
nojsxEV4MDwLohPrI0Aq9Z/ymFIG3phLiiewLEct52BGGWNAA9D+W5JyKch8XOHZ
BuODMZ+K423Q5rsKauH9CEfyQnR9IlEuy2s/OipblfHzQiUMW/xRoErxK9712DKF
tHYINz/B6b9t+Oqlc0+VjQV3QYIShctiOWUwnG8CgYAuq53MvdZB0lPnVcahL2R6
E0qGWDwu/GMArgdWAy5yX2t1r40UgLNoeoL4wYq4hlZGmsdHN+PMUO8jXHmkvNW2
SgSufWnZicEnL6qC9wrc5GIubmGtQUFarJOhGCECZPOQbq27ZAmrVpNq7wzfoZYe
57uwyl8rEobOoFBK/CurFQKBgH967KuT2VXr/30fjoPM77GYPzn7+xUjRtPfNuPg
pfunbHjEFZq2QiTbmm9EgTFSDkOiqCj9tx2pDeaWWiPWyywBK8GPw3G+DjUdNwls
6p5iJm66vtyKlpPbEZgEdj6RX1kGisVKPbwFCo2GrIA9/Ig4NQEBauNUMNknscuJ
pK7ZAoGASCyceEUCCcrO8Kn8mbJoTFHhnHDhP+W7Px0+bfkppbrdxp1rqM9eGOTa
9d/lXTkLuto7xK5lEUSralBcuDz5UpDwnJVDPKtPYpDYUpt84Nf7qOvyjKATMSkr
TR21kbl8mPTdQhVp9+9N756yOL5EoI6KyMtYx3qzpWyGz6Ix1vs=
-----END RSA PRIVATE KEY-----";

    const RSA_PUBLIC_KEY_PEM: &[u8] = b"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0RNM7roNOPOvflr5JfMs
vHSqlbNo0zorU6GSq1kzj5aHPzomq6+IOpEEPYVqz+uGOkI4z9uUrc5e+OnN0y6s
/3IKzC5rvTn9mu34jQGFG9wvJn4jnUhTXi0onQpNQeQjT/bNcYaE1QNu/xZWloaG
L2h7lMPkgKCUrMvMhfgmHjYl1wyUpSzmyQBSHvRQwpit06RE2zIpaDz/BexIzedQ
I8rNFqwb6JCwyzqLHvFfi+Z9bIsk5hypOOJqmn+t5RL3RPO6FSAlS4Gmo4D6TYgu
DKctQgxrjkNFDntkaZ5CkhZBaEFQMxodcPRAFpey7c4Ps0SHtYEiIvt5KunEQdxW
VQIDAQAB
-----END PUBLIC KEY-----";

    const EC_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgQpX/DEI9Y5T4fy9g
faOzjWWvb8W/gmtENdKWZLL8gFKhRANCAATUGEhVz6Sx8z7c4f/r1CtOvvIiQEuj
nkfEnv/3inIgPMiQdSgaGZS2hJHjcA1jLZ2Ymf/tzEPU+/6o60PqdG5J
-----END PRIVATE KEY-----";

    const EC_PUBLIC_KEY_PEM: &[u8] = b"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE1BhIVc+ksfM+3OH/69QrTr7yIkBL
o55HxJ7/94pyIDzIkHUoGhmUtoSR43ANYy2dmJn/7cxD1Pv+qOtD6nRuSQ==
-----END PUBLIC KEY-----";

    const ED_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEILXRgqo5hf7tNsxH4J4mDZer9WVmeSxTHqF/Hzj7xdKd
-----END PRIVATE KEY-----";

    const ED_PUBLIC_KEY_PEM: &[u8] = b"-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAUOxZ1ei26f974AmcJc9sSe+sEtApcXqYgu+cGBoC7jw=
-----END PUBLIC KEY-----";

    /// JWK generated from `RSA_PRIVATE_KEY_PEM`
    /// @see https://russelldavies.github.io/jwk-creator/
    const JWK_JSON: &[u8] = br#"{"jwk":
[
    {
        "kty": "RSA",
        "n": "0RNM7roNOPOvflr5JfMsvHSqlbNo0zorU6GSq1kzj5aHPzomq6-IOpEEPYVqz-uGOkI4z9uUrc5e-OnN0y6s_3IKzC5rvTn9mu34jQGFG9wvJn4jnUhTXi0onQpNQeQjT_bNcYaE1QNu_xZWloaGL2h7lMPkgKCUrMvMhfgmHjYl1wyUpSzmyQBSHvRQwpit06RE2zIpaDz_BexIzedQI8rNFqwb6JCwyzqLHvFfi-Z9bIsk5hypOOJqmn-t5RL3RPO6FSAlS4Gmo4D6TYguDKctQgxrjkNFDntkaZ5CkhZBaEFQMxodcPRAFpey7c4Ps0SHtYEiIvt5KunEQdxWVQ",
        "e": "AQAB",
        "d": "Bsd2biMwPJGCNjmi7H0XDUFHKrV3h6nSL9MEW3P3_QbnecBb8qMtaqC7H3eNABLl9kuhZfP7vSr831tNZjSUPXAoM3ip26fetkBTF9ddxop8KOZcJ0RqF5qlP3pPCPzpvDUHck_nTLTgy3qg8h6IiU78-tTjOtx6FaRCDHIAYmhWDua9u4q3Bz_kuG_Sj_02iGDMezA27hX9MrpqG6OTG4ncUgtpq0Ra-lT87IT5GP4lssjn0hHt08100kWZblRtCPnk3Bu8emKgLXWPt5C7veScJDXZnnKB3N83OlDbfot_zJ1eBHsaes1PUXDsl_F-vpsTepQrU23NrTjzfXiefQ",
        "p": "6M7x3EupeldUF7of26P7qfKbDubVx4DWcFWUmNCoxj4T0Zr2m-scHvr2SC8lBg-kl7IEU2GuUio73XbXiGhVfDHwz62LTPW5owgFqb-n2LPfKdOnN5c4E-ms8lEFxyQCIaL6x3zYMtKnDw1JC3dm7onGGXMAnNoeL5r3X67CI3s",
        "q": "5ecenojsxEV4MDwLohPrI0Aq9Z_ymFIG3phLiiewLEct52BGGWNAA9D-W5JyKch8XOHZBuODMZ-K423Q5rsKauH9CEfyQnR9IlEuy2s_OipblfHzQiUMW_xRoErxK9712DKFtHYINz_B6b9t-Oqlc0-VjQV3QYIShctiOWUwnG8",
        "dp": "LqudzL3WQdJT51XGoS9kehNKhlg8LvxjAK4HVgMucl9rda-NFICzaHqC-MGKuIZWRprHRzfjzFDvI1x5pLzVtkoErn1p2YnBJy-qgvcK3ORiLm5hrUFBWqyToRghAmTzkG6tu2QJq1aTau8M36GWHue7sMpfKxKGzqBQSvwrqxU",
        "dq": "f3rsq5PZVev_fR-Og8zvsZg_Ofv7FSNG09824-Cl-6dseMQVmrZCJNuab0SBMVIOQ6KoKP23HakN5pZaI9bLLAErwY_Dcb4ONR03CWzqnmImbrq-3IqWk9sRmAR2PpFfWQaKxUo9vAUKjYasgD38iDg1AQFq41Qw2Sexy4mkrtk",
        "qi": "SCyceEUCCcrO8Kn8mbJoTFHhnHDhP-W7Px0-bfkppbrdxp1rqM9eGOTa9d_lXTkLuto7xK5lEUSralBcuDz5UpDwnJVDPKtPYpDYUpt84Nf7qOvyjKATMSkrTR21kbl8mPTdQhVp9-9N756yOL5EoI6KyMtYx3qzpWyGz6Ix1vs",
        "alg": "RS512",
        "kid": "test_key",
        "use": "sig"
      }
]
}"#;

    fn create_rsa_key_pair() -> (EncodingKey, NamedTempFile) {
        (
            EncodingKey::from_rsa_pem(RSA_PRIVATE_KEY_PEM).unwrap(),
            save_to_temp_file(RSA_PUBLIC_KEY_PEM),
        )
    }

    fn create_ec_key_pair() -> (EncodingKey, NamedTempFile) {
        (
            EncodingKey::from_ec_pem(EC_PRIVATE_KEY_PEM).unwrap(),
            save_to_temp_file(EC_PUBLIC_KEY_PEM),
        )
    }

    fn create_ed_key_pair() -> (EncodingKey, NamedTempFile) {
        (
            EncodingKey::from_ed_pem(ED_PRIVATE_KEY_PEM).unwrap(),
            save_to_temp_file(ED_PUBLIC_KEY_PEM),
        )
    }

    fn save_to_temp_file(key: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();

        file.write_all(key).unwrap();

        file
    }

    fn create_token(header: &Header, claims: &str, key: &EncodingKey) -> String {
        let parsed: serde_json::Value = serde_json::from_str(claims).unwrap();
        encode(&header, &parsed, key).unwrap()
    }

    fn get_valid_exp_claim() -> u64 {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        timestamp + 3600
    }

    #[test]
    fn test_validate_rsa_token() {
        let (encoding_key, decoding_key_path) = create_rsa_key_pair();

        let conf = JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
            ec_public_key: None,
            ed_public_key: None,
        };

        let user = JwtUserConfig {
            claims: HashMap::new(),
        };

        let header = Header::new(Algorithm::RS512);
        let exp = get_valid_exp_claim();
        let token = create_token(
            &header,
            &format!(r#"{{"sub": "foo", "exp": {exp}}}"#),
            &encoding_key,
        );

        let provider = JwtAuthPorvider::new(conf).unwrap();
        let ctx = provider.authenticate(&user, &token).unwrap();

        assert_eq!(ctx.raw_token, token);
        assert_eq!(ctx.header, header);
        assert_eq!(ctx.header, header);
        assert_eq!(ctx.claims.get("sub"), Some(&Value::String("foo".into())));
        assert_eq!(ctx.claims.get("exp"), Some(&Value::Number(exp.into())));

        // should reject invalid token
        let invalid = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.NHVaYe26MbtOYhSKkoKYdFVomg4i8ZJd8_-RU8VNbftc4TSMb4bXP3l3YlNWACwyXPGffz5aXHc6lty1Y2t4SWRqGteragsVdZufDn5BlnJl9pdR_kdVFUsra2rWKEofkZeIC4yWytE58sMIihvo9H1ScmmVwBcQP6XETqYd0aSHp1gOa9RdUPDvoXQ5oqygTqVtxaDr6wUFKrKItgBMzWIdNZ6y7O9E0DhEPTbE9rfBo6KTFsHAZnMg4k68CDp2woYIaXbmYTWcvbzIuHO7_37GT79XdIwkm95QJ7hYC9RiwrV7mesbY4PAahERJawntho0my942XheVLmGwLMBkQ";
        provider.authenticate(&user, &invalid).unwrap_err();
    }

    #[test]
    fn test_validate_ec_token() {
        let (encoding_key, decoding_key_path) = create_ec_key_pair();

        let conf = JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: None,
            ec_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
            ed_public_key: None,
        };

        let user = JwtUserConfig {
            claims: HashMap::new(),
        };

        let header = Header::new(Algorithm::ES256);
        let exp = get_valid_exp_claim();
        let token = create_token(
            &header,
            &format!(r#"{{"sub": "foo", "exp": {exp}}}"#),
            &encoding_key,
        );

        let provider = JwtAuthPorvider::new(conf).unwrap();
        let ctx = provider.authenticate(&user, &token).unwrap();

        assert_eq!(ctx.raw_token, token);
        assert_eq!(ctx.header, header);
        assert_eq!(ctx.header, header);
        assert_eq!(ctx.claims.get("sub"), Some(&Value::String("foo".into())));
        assert_eq!(ctx.claims.get("exp"), Some(&Value::Number(exp.into())));

        // should reject invalid token
        let invalid = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.tyh-VfuzIxCyGYDlkBA7DfyjrqmSHu6pQ2hoZuFqUSLPNY2N0mpHb3nk5K17HWP_3cYHBw7AhHale5wky6-sVA";
        provider.authenticate(&user, &invalid).unwrap_err();
    }

    #[test]
    fn test_validate_ed_token() {
        let (encoding_key, decoding_key_path) = create_ed_key_pair();

        let conf = JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: None,
            ec_public_key: None,
            ed_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
        };

        let user = JwtUserConfig {
            claims: HashMap::new(),
        };

        let header = Header::new(Algorithm::EdDSA);
        let exp = get_valid_exp_claim();
        let token = create_token(
            &header,
            &format!(r#"{{"sub": "foo", "exp": {exp}}}"#),
            &encoding_key,
        );

        let provider = JwtAuthPorvider::new(conf).unwrap();
        let ctx = provider.authenticate(&user, &token).unwrap();

        assert_eq!(ctx.raw_token, token);
        assert_eq!(ctx.header, header);
        assert_eq!(ctx.header, header);
        assert_eq!(ctx.claims.get("sub"), Some(&Value::String("foo".into())));
        assert_eq!(ctx.claims.get("exp"), Some(&Value::Number(exp.into())));

        // should reject invalid token
        let invalid = "eyJhbGciOiJFUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.AbVUinMiT3J_03je8WTOIl-VdggzvoFgnOsdouAs-DLOtQzau9valrq-S6pETyi9Q18HH-EuwX49Q7m3KC0GuNBJAc9Tksulgsdq8GqwIqZqDKmG7hNmDzaQG1Dpdezn2qzv-otf3ZZe-qNOXUMRImGekfQFIuH_MjD2e8RZyww6lbZk";
        provider.authenticate(&user, &invalid).unwrap_err();
    }

    #[test]
    fn test_validate_jwk_token() {
        let (encoding_key, _) = create_rsa_key_pair();
        let jwk_path = save_to_temp_file(JWK_JSON);

        let conf = JwtAuthProviderConfig {
            jwk: Some(format!("file://{}", jwk_path.path().to_str().unwrap())),
            rsa_public_key: None,
            ec_public_key: None,
            ed_public_key: None,
        };

        let user = JwtUserConfig {
            claims: HashMap::new(),
        };

        let header = Header::new(Algorithm::RS512);
        let exp = get_valid_exp_claim();
        let token = create_token(
            &header,
            &format!(r#"{{"sub": "foo", "exp": {exp}}}"#),
            &encoding_key,
        );

        let provider = JwtAuthPorvider::new(conf).unwrap();
        let ctx = provider.authenticate(&user, &token).unwrap();

        assert_eq!(ctx.raw_token, token);
        assert_eq!(ctx.header, header);
        assert_eq!(ctx.header, header);
        assert_eq!(ctx.claims.get("sub"), Some(&Value::String("foo".into())));
        assert_eq!(ctx.claims.get("exp"), Some(&Value::Number(exp.into())));
    }

    #[test]
    fn test_validate_claim_check() {
        let (encoding_key, decoding_key_path) = create_ed_key_pair();

        let conf = JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: None,
            ec_public_key: None,
            ed_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
        };

        let user = JwtUserConfig {
            claims: [("sub".into(), TokenClaimCheck::Eq("bar".into()))]
                .into_iter()
                .collect(),
        };

        let header = Header::new(Algorithm::EdDSA);
        let exp = get_valid_exp_claim();
        let token = create_token(
            &header,
            &format!(r#"{{"sub": "foo", "exp": {exp}}}"#),
            &encoding_key,
        );

        let provider = JwtAuthPorvider::new(conf).unwrap();
        provider.authenticate(&user, &token).unwrap_err();
    }

    #[test]
    fn test_validate_exp_claim() {
        let (encoding_key, decoding_key_path) = create_ed_key_pair();

        let conf = JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: None,
            ec_public_key: None,
            ed_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
        };

        let user = JwtUserConfig {
            claims: HashMap::new(),
        };

        let header = Header::new(Algorithm::EdDSA);
        let exp = 1234;
        let token = create_token(
            &header,
            &format!(r#"{{"sub": "foo", "exp": {exp}}}"#),
            &encoding_key,
        );

        let provider = JwtAuthPorvider::new(conf).unwrap();
        provider.authenticate(&user, &token).unwrap_err();
    }
}
