/// Gets the bincode configuration used to serialise types
/// between postgres and ansilo
pub fn bincode_conf() -> bincode::config::Configuration {
    bincode::config::standard()
}