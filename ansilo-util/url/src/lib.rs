use ansilo_core::err::{bail, Context, Error, Result};
use reqwest::Url;

mod file;
mod http;
mod shell;

/// Retrieves the contents from the supplied URL.
///
/// We current support http(s):// and file:// protocols
pub fn get(url: impl Into<String>) -> Result<Vec<u8>> {
    let url: String = url.into();
    let url = Url::parse(&url).with_context(|| format!("Failed to parse URL: {}", url))?;

    match url.scheme() {
        "http" | "https" => http::get_http(url),
        "file" => file::get_file(
            url.to_file_path()
                .map_err(|_| Error::msg("Failed to get file path from URL"))?,
        ),
        "sh" => shell::get_shell(
            url.to_file_path()
                .map_err(|_| Error::msg("Failed to get file path from URL"))?,
            url.query_pairs().find_map(|(k, v)| {
                if k == "args" {
                    Some(v.to_string())
                } else {
                    None
                }
            }),
        ),
        _ => bail!(
            "Unsupported URL protocol '{}' in url: {}",
            url.scheme(),
            url
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

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

    #[test]
    fn test_sh_run_bin_true() {
        assert_eq!(get("sh:///bin/true").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn test_sh_run_echo_with_args() {
        assert_eq!(
            get("sh:///bin/echo?args=hello world").unwrap(),
            b"hello world\n".to_vec()
        );
    }

    #[test]
    fn test_sh_run_echo_with_args_url_encoding() {
        assert_eq!(
            get("sh:///bin/echo?args=hello+world").unwrap(),
            b"hello world\n".to_vec()
        );

        assert_eq!(
            get("sh:///bin/echo?args=hello%20world").unwrap(),
            b"hello world\n".to_vec()
        );
        assert_eq!(
            get("sh:///bin/echo?args=hello+world").unwrap(),
            b"hello world\n".to_vec()
        );
        assert_eq!(
            get("sh:///bin/echo?args=hello+world").unwrap(),
            b"hello world\n".to_vec()
        );
    }

    #[test]
    fn test_sh_run_bin_false() {
        assert_eq!(
            get("sh:///bin/false").unwrap_err().to_string(),
            "Running process '/bin/false' failed with exit code: Some(1)"
        );
    }

    #[test]
    fn test_sh_run_invalid_path() {
        assert_eq!(
            get("sh:///non/existant/test").unwrap_err().to_string(),
            "Failed to spawn '/non/existant/test', please check file exists and has correct permissions"
        );
    }
}
