use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::Value;

pub fn valid_exp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 1800
}

pub fn make_rsa_token(claims: Value, private_key: &[u8]) -> String {
    encode(
        &Header::new(Algorithm::RS512),
        &claims,
        &EncodingKey::from_rsa_pem(private_key).unwrap(),
    )
    .unwrap()
}
