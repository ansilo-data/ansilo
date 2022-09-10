use std::{
    io::Write,
    time::{SystemTime, UNIX_EPOCH},
};

use jsonwebtoken::{encode, EncodingKey, Header};
use tempfile::NamedTempFile;

pub use jsonwebtoken;
pub use tempfile;

pub const RSA_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN RSA PRIVATE KEY-----
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

pub const RSA_PUBLIC_KEY_PEM: &[u8] = b"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0RNM7roNOPOvflr5JfMs
vHSqlbNo0zorU6GSq1kzj5aHPzomq6+IOpEEPYVqz+uGOkI4z9uUrc5e+OnN0y6s
/3IKzC5rvTn9mu34jQGFG9wvJn4jnUhTXi0onQpNQeQjT/bNcYaE1QNu/xZWloaG
L2h7lMPkgKCUrMvMhfgmHjYl1wyUpSzmyQBSHvRQwpit06RE2zIpaDz/BexIzedQ
I8rNFqwb6JCwyzqLHvFfi+Z9bIsk5hypOOJqmn+t5RL3RPO6FSAlS4Gmo4D6TYgu
DKctQgxrjkNFDntkaZ5CkhZBaEFQMxodcPRAFpey7c4Ps0SHtYEiIvt5KunEQdxW
VQIDAQAB
-----END PUBLIC KEY-----";

pub const EC_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgQpX/DEI9Y5T4fy9g
faOzjWWvb8W/gmtENdKWZLL8gFKhRANCAATUGEhVz6Sx8z7c4f/r1CtOvvIiQEuj
nkfEnv/3inIgPMiQdSgaGZS2hJHjcA1jLZ2Ymf/tzEPU+/6o60PqdG5J
-----END PRIVATE KEY-----";

pub const EC_PUBLIC_KEY_PEM: &[u8] = b"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE1BhIVc+ksfM+3OH/69QrTr7yIkBL
o55HxJ7/94pyIDzIkHUoGhmUtoSR43ANYy2dmJn/7cxD1Pv+qOtD6nRuSQ==
-----END PUBLIC KEY-----";

pub const ED_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEILXRgqo5hf7tNsxH4J4mDZer9WVmeSxTHqF/Hzj7xdKd
-----END PRIVATE KEY-----";

pub const ED_PUBLIC_KEY_PEM: &[u8] = b"-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEAUOxZ1ei26f974AmcJc9sSe+sEtApcXqYgu+cGBoC7jw=
-----END PUBLIC KEY-----";

/// JWK generated from `RSA_PRIVATE_KEY_PEM`
/// @see https://russelldavies.github.io/jwk-creator/
pub const JWK_JSON: &[u8] = br#"{"keys":
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

pub fn create_rsa_key_pair() -> (EncodingKey, NamedTempFile) {
    (
        EncodingKey::from_rsa_pem(RSA_PRIVATE_KEY_PEM).unwrap(),
        save_to_temp_file(RSA_PUBLIC_KEY_PEM),
    )
}

pub fn create_ec_key_pair() -> (EncodingKey, NamedTempFile) {
    (
        EncodingKey::from_ec_pem(EC_PRIVATE_KEY_PEM).unwrap(),
        save_to_temp_file(EC_PUBLIC_KEY_PEM),
    )
}

pub fn create_ed_key_pair() -> (EncodingKey, NamedTempFile) {
    (
        EncodingKey::from_ed_pem(ED_PRIVATE_KEY_PEM).unwrap(),
        save_to_temp_file(ED_PUBLIC_KEY_PEM),
    )
}

pub fn save_to_temp_file(key: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();

    file.write_all(key).unwrap();

    file
}

pub fn create_token(header: &Header, claims: &str, key: &EncodingKey) -> String {
    let parsed: serde_json::Value = serde_json::from_str(claims).unwrap();
    encode(&header, &parsed, key).unwrap()
}

pub fn get_valid_exp_claim() -> u64 {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    timestamp + 3600
}
