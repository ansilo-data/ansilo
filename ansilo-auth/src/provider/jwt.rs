use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use ansilo_core::{
    auth::JwtAuthContext,
    config::{JwtAuthProviderConfig, JwtUserConfig},
    err::{bail, ensure, Context, Error, Result},
};
use ansilo_logging::{info, warn};
use jsonwebkey::KeyUse;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, TokenData, Validation};
use serde::Deserialize;

use crate::provider::check::validate_jwt_claim;

/// Used for validating JWT tokens.
pub struct JwtAuthProvider {
    /// Provider config
    _conf: &'static JwtAuthProviderConfig,
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

impl JwtAuthProvider {
    pub fn new(conf: &'static JwtAuthProviderConfig) -> Result<Self> {
        ensure!(
            conf.jwk.is_some() || conf.rsa_public_key.is_some() || conf.ec_public_key.is_some()  || conf.ed_public_key.is_some(),
            "Must specify either 'jwk', 'rsa_public_key', 'ec_public_key', or 'ed_public_key' options for jwt auth provider"
        );

        let keys = Self::retrieve_decoding_keys(conf)?;
        let state = Arc::new(Mutex::new(State::new(keys)));

        Self::periodically_update_keys(conf, Arc::clone(&state));

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
            validate_jwt_claim(claim, actual, check)?;
        }

        let header = serde_json::to_value(header).context("Failed to serialise token header")?;

        // Token verified and passes checks
        Ok(JwtAuthContext {
            raw_token: jwt.to_string(),
            header,
            claims: decoded_token.claims,
        })
    }

    /// Retrieves a new decoding key
    fn retrieve_decoding_keys(
        conf: &'static JwtAuthProviderConfig,
    ) -> Result<Vec<VerificationKey>> {
        if let Some(jwk_url) = conf.jwk.as_ref() {
            #[derive(Deserialize)]
            struct JwkSet {
                keys: Vec<jsonwebkey::JsonWebKey>,
            }

            info!("Retrieving JWK's from {}", jwk_url);
            let data = ansilo_util_url::get(jwk_url).context("Failed to get JWK")?;
            let set: JwkSet =
                serde_json::from_slice(data.as_slice()).context("Failed to parse JWK JSON")?;

            ensure!(set.keys.len() > 0, "Found empty JWK set");

            return set
                .keys
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

    fn periodically_update_keys(conf: &'static JwtAuthProviderConfig, state: Arc<Mutex<State>>) {
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

impl Drop for JwtAuthProvider {
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
    use ansilo_core::config::TokenClaimCheck;
    use jsonwebtoken::Header;
    use serde_json::Value;

    use super::*;
    use crate::provider::jwt_test::*;

    #[test]
    fn test_validate_rsa_token() {
        let (encoding_key, decoding_key_path) = create_rsa_key_pair();

        let conf = Box::leak(Box::new(JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
            ec_public_key: None,
            ed_public_key: None,
            login: None,
        }));

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

        let provider = JwtAuthProvider::new(conf).unwrap();
        let ctx = provider.authenticate(&user, &token).unwrap();

        assert_eq!(ctx.raw_token, token);
        assert_eq!(
            ctx.header.as_object().unwrap()["alg"],
            Value::String("RS512".into())
        );
        assert_eq!(ctx.claims.get("sub"), Some(&Value::String("foo".into())));
        assert_eq!(ctx.claims.get("exp"), Some(&Value::Number(exp.into())));

        // should reject invalid token
        let invalid = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.NHVaYe26MbtOYhSKkoKYdFVomg4i8ZJd8_-RU8VNbftc4TSMb4bXP3l3YlNWACwyXPGffz5aXHc6lty1Y2t4SWRqGteragsVdZufDn5BlnJl9pdR_kdVFUsra2rWKEofkZeIC4yWytE58sMIihvo9H1ScmmVwBcQP6XETqYd0aSHp1gOa9RdUPDvoXQ5oqygTqVtxaDr6wUFKrKItgBMzWIdNZ6y7O9E0DhEPTbE9rfBo6KTFsHAZnMg4k68CDp2woYIaXbmYTWcvbzIuHO7_37GT79XdIwkm95QJ7hYC9RiwrV7mesbY4PAahERJawntho0my942XheVLmGwLMBkQ";
        provider.authenticate(&user, &invalid).unwrap_err();
    }

    #[test]
    fn test_validate_ec_token() {
        let (encoding_key, decoding_key_path) = create_ec_key_pair();

        let conf = Box::leak(Box::new(JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: None,
            ec_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
            ed_public_key: None,
            login: None,
        }));

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

        let provider = JwtAuthProvider::new(conf).unwrap();
        let ctx = provider.authenticate(&user, &token).unwrap();

        assert_eq!(ctx.raw_token, token);
        assert_eq!(
            ctx.header.as_object().unwrap()["alg"],
            Value::String("ES256".into())
        );
        assert_eq!(ctx.claims.get("sub"), Some(&Value::String("foo".into())));
        assert_eq!(ctx.claims.get("exp"), Some(&Value::Number(exp.into())));

        // should reject invalid token
        let invalid = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.tyh-VfuzIxCyGYDlkBA7DfyjrqmSHu6pQ2hoZuFqUSLPNY2N0mpHb3nk5K17HWP_3cYHBw7AhHale5wky6-sVA";
        provider.authenticate(&user, &invalid).unwrap_err();
    }

    #[test]
    fn test_validate_ed_token() {
        let (encoding_key, decoding_key_path) = create_ed_key_pair();

        let conf = Box::leak(Box::new(JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: None,
            ec_public_key: None,
            ed_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
            login: None,
        }));

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

        let provider = JwtAuthProvider::new(conf).unwrap();
        let ctx = provider.authenticate(&user, &token).unwrap();

        assert_eq!(ctx.raw_token, token);
        assert_eq!(
            ctx.header.as_object().unwrap()["alg"],
            Value::String("EdDSA".into())
        );
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

        let conf = Box::leak(Box::new(JwtAuthProviderConfig {
            jwk: Some(format!("file://{}", jwk_path.path().to_str().unwrap())),
            rsa_public_key: None,
            ec_public_key: None,
            ed_public_key: None,
            login: None,
        }));

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

        let provider = JwtAuthProvider::new(conf).unwrap();
        let ctx = provider.authenticate(&user, &token).unwrap();

        assert_eq!(ctx.raw_token, token);
        assert_eq!(
            ctx.header.as_object().unwrap()["alg"],
            Value::String("RS512".into())
        );
        assert_eq!(ctx.claims.get("sub"), Some(&Value::String("foo".into())));
        assert_eq!(ctx.claims.get("exp"), Some(&Value::Number(exp.into())));
    }

    #[test]
    fn test_validate_claim_check() {
        let (encoding_key, decoding_key_path) = create_ed_key_pair();

        let conf = Box::leak(Box::new(JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: None,
            ec_public_key: None,
            ed_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
            login: None,
        }));

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

        let provider = JwtAuthProvider::new(conf).unwrap();
        provider.authenticate(&user, &token).unwrap_err();
    }

    #[test]
    fn test_validate_exp_claim() {
        let (encoding_key, decoding_key_path) = create_ed_key_pair();

        let conf = Box::leak(Box::new(JwtAuthProviderConfig {
            jwk: None,
            rsa_public_key: None,
            ec_public_key: None,
            ed_public_key: Some(format!(
                "file://{}",
                decoding_key_path.path().to_str().unwrap()
            )),
            login: None,
        }));

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

        let provider = JwtAuthProvider::new(conf).unwrap();
        provider.authenticate(&user, &token).unwrap_err();
    }
}
