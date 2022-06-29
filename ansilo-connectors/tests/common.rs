use std::{
    collections::HashMap,
    net::{IpAddr, TcpStream},
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::Duration, env,
};

pub struct ContainerInstances {
    pub services: HashMap<String, IpAddr>,
    infra_path: PathBuf,
}

impl Drop for ContainerInstances {
    fn drop(&mut self) {
        stop_containers_ecs(self.infra_path.clone())
    }
}

/// Starts the contains described by {infra_path}/docker-compose.yml
/// Returns a hash map of the service names mapped to their respective ip addresses
pub fn start_containers(infra_path: PathBuf) -> ContainerInstances {
    start_containers_ecs(infra_path)
}

fn start_containers_ecs(infra_path: PathBuf) -> ContainerInstances {
    let status = Command::new("ecs-cli")
        .args(&["compose", "up", "--create-log-groups"])
        .current_dir(infra_path.clone())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    if !status.success() {
        panic!("Failed to create containers on ECS");
    }

    // get service private ip addresses
    let tasks = Command::new("bash")
        .args(&[
            "-c",
            "ecs-cli compose ps | tail -n +2 | awk '{print $1 \" \" $3}'",
        ])
        .current_dir(infra_path.clone())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
        .wait_with_output()
        .unwrap();

    let services = String::from_utf8_lossy(&tasks.stdout[..])
        .lines()
        .map(|i| i.split(' ').map(|i| i.to_owned()).collect::<Vec<String>>())
        .map(|i| {
            (
                parse_task_name(i[0].clone()),
                parse_service_port(i[1].clone()),
            )
        })
        .map(|((cluster, task_id, service), port)| {
            let ip_addr = get_task_private_ip(cluster, task_id);
            if let Some(port) = port {
                println!("Waiting for {service} service to come online");
                wait_for_port_open(ip_addr, port);
            }
            (service, ip_addr)
        })
        .collect::<HashMap<String, IpAddr>>();

    ContainerInstances {
        services,
        infra_path,
    }
}

/// Parses the task name from ecs-cli into (cluster, task_id, service)
fn parse_task_name(name: String) -> (String, String, String) {
    let parts = name
        .split('/')
        .map(|i| i.to_owned())
        .collect::<Vec<String>>();

    (
        parts[0].to_owned(),
        parts[1].to_owned(),
        parts[2].to_owned(),
    )
}

/// Parses the external port mappings from ecs-cli
fn parse_service_port(port_mapping: String) -> Option<u16> {
    if !port_mapping.contains(':') || !port_mapping.contains("->") {
        return None;
    }

    let port = &port_mapping[port_mapping.find(':').unwrap() + 1..port_mapping.find("->").unwrap()];
    port.parse().ok()
}

fn get_task_private_ip(cluster: String, task_id: String) -> IpAddr {
    let output = Command::new("bash")
        .args(&["-c".to_string(), format!("aws ecs describe-tasks --tasks {} --cluster {cluster} --query 'tasks[0].attachments[0].details[?name==`privateIPv4Address`].value' --output text", task_id.clone())])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
        .wait_with_output()
        .unwrap();

    let ip_str = String::from_utf8_lossy(&output.stdout[..])
        .trim()
        .to_string();
    println!("Private IP from {}: {:?}", task_id, ip_str);
    ip_str.parse().unwrap()
}

fn wait_for_port_open(ip_addr: IpAddr, port: u16) {
    let addr = (ip_addr, port).into();

    loop {
        println!("Checking if {ip_addr}:{port} is listening...");

        if TcpStream::connect_timeout(&addr, Duration::from_secs(5)).is_ok() {
            println!("Port is open, continuing!");
            break;
        }

        thread::sleep(Duration::from_secs(5));
    }
}

fn stop_containers_ecs(infra_path: PathBuf) {
    Command::new("ecs-cli")
        .args(&["compose", "down"])
        .current_dir(infra_path.clone())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
        .wait()
        .unwrap();
}

pub fn get_current_target_dir() -> PathBuf {
    env::current_exe()
        .and_then(|mut p| {
            while p
                .parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy()
                != "target"
            {
                p = p.parent().unwrap().to_path_buf();
            }

            Ok(p)
        })
        .unwrap()
}
