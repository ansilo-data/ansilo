use ansilo_main::Ansilo;

/// Gets the http url to the web api
pub fn url(instance: &Ansilo, path: &str) -> String {
    url_proto(instance, "http", path)
}

/// Gets the https url to the web api
pub fn url_https(instance: &Ansilo, path: &str) -> String {
    url_proto(instance, "https", path)
}

/// Gets the url to the web api
pub fn url_proto(instance: &Ansilo, proto: &str, path: &str) -> String {
    let port = instance.subsystems().unwrap().proxy().addrs().unwrap()[0].port();
    format!("{proto}://localhost:{port}{path}")
}
