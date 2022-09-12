use std::time::Duration;

use ansilo_core::err::{Context, Result};
use reqwest::Url;

/// Gets response body from the supplied http(s) url
pub(crate) fn get_http(url: Url) -> Result<Vec<u8>> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .user_agent("Ansilo/v1")
        .build()
        .context("Failed to build http client")?;

    let response = client
        .get(url.clone())
        .timeout(Duration::from_secs(30))
        .send()
        .with_context(|| format!("Error during request to {}", url))?;

    let response = response.error_for_status()?;

    Ok(response.bytes()?.to_vec())
}
