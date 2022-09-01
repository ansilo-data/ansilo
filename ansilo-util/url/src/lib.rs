use std::{fs, path::PathBuf, time::Duration};

use ansilo_core::err::{bail, Context, Error, Result};
use reqwest::Url;

/// Retrieves the contents from the supplied URL.
///
/// We current support http(s):// and file:// protocols
pub fn get(url: impl Into<String>) -> Result<Vec<u8>> {
    let url: String = url.into();
    let url = Url::parse(&url).with_context(|| format!("Failed to parse URL: {}", url))?;

    match url.scheme() {
        "http" | "https" => get_http(url),
        "file" => get_file(
            url.to_file_path()
                .map_err(|_| Error::msg("Failed to get file path from URL"))?,
        ),
        _ => bail!(
            "Unsupported URL protocol '{}' in url: {}",
            url.scheme(),
            url
        ),
    }
}

/// Gets response body from the supplied http(s) url
fn get_http(url: Url) -> Result<Vec<u8>> {
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

/// Gets the file contents from the supplied file url
fn get_file(path: PathBuf) -> Result<Vec<u8>> {
    fs::read(&path).with_context(|| format!("Failed to read file {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_get_invalid() {
        get("invalid").unwrap_err();
        get("invalid://foo").unwrap_err();
        get("ssh://foo:123/bar").unwrap_err();
    }

    #[test]
    fn test_url_get_http() {
        assert_eq!(
            get("http://httpbin.org/status/200").unwrap(),
            Vec::<u8>::new()
        );
        assert_eq!(
            get("http://httpbin.org/base64/Zm9vYmFy").unwrap(),
            "foobar".as_bytes().to_vec()
        );
    }

    #[test]
    fn test_url_get_http_error_status() {
        get("http://httpbin.org/status/500").unwrap_err();
    }

    #[test]
    fn test_url_get_https() {
        assert_eq!(
            get("https://httpbin.org/status/200").unwrap(),
            Vec::<u8>::new()
        );
        assert_eq!(
            get("https://httpbin.org/base64/Zm9vYmFy").unwrap(),
            "foobar".as_bytes().to_vec()
        );
    }

    #[test]
    fn test_url_get_https_redirect() {
        assert_eq!(
            get(
                "https://httpbin.org/redirect-to?url=https%3A%2F%2Fhttpbin.org%2Fbase64%2FZm9vYmFy"
            )
            .unwrap(),
            "foobar".as_bytes().to_vec()
        );
    }

    #[test]
    fn test_url_get_https_error_status() {
        get("https://httpbin.org/status/500").unwrap_err();
    }

    #[test]
    fn test_url_get_file_invalid() {
        get("file://httpbin.org/status/500").unwrap_err();
    }

    #[test]
    fn test_url_get_file_non_existant() {
        get("file:///this/do/not/exist/i/hope").unwrap_err();
    }

    #[test]
    fn test_url_get_file() {
        fs::write("/tmp/ansilo-example-file.txt", "file content").unwrap();
        assert_eq!(
            get("file:///tmp/ansilo-example-file.txt").unwrap(),
            "file content".as_bytes().to_vec()
        );
    }
}
