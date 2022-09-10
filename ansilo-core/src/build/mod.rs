const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
const BUILD_ID: Option<&'static str> = option_env!("ANSILO_BUILD_ID");

/// Gets the current ansilo version id
pub fn ansilo_version() -> String {
    format!(
        "{}-{}",
        VERSION.unwrap_or("unknown"),
        BUILD_ID.unwrap_or("unknown")
    )
}
