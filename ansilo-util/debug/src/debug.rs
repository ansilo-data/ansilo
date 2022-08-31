/// Doesn't seem to work with VS Code remote :(
/// @see https://github.com/microsoft/vscode-remote-release/issues/4260
#[macro_export]
macro_rules! breakpoint {
    () => {
        let url = format!("vscode://vadimcn.vscode-lldb/launch/config?{{'request':'attach','pid':{}}}", std::process::id());
        std::process::Command::new("code").arg("--open-url").arg(url).output().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1000)); 
    };
}