use std::{
    collections::HashMap,
    env,
    net::{IpAddr, TcpStream},
    path::PathBuf,
    process::{Command, Stdio},
    sync::mpsc::channel,
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

#[macro_export]
macro_rules! current_dir {
    () => {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join(file!())
            .parent()
            .unwrap()
            .to_owned()
    };
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ContainerInstances {
    instances: HashMap<String, Instance>,
    infra_path: PathBuf,
    stop_on_drop: bool,
}

impl ContainerInstances {
    pub fn get(&self, service: impl Into<String>) -> Option<&Instance> {
        self.instances.get(&service.into())
    }
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: String,
    pub ip: IpAddr,
}

impl Instance {
    pub fn new(id: String, ip: IpAddr) -> Self {
        Self { id, ip }
    }
}

impl Drop for ContainerInstances {
    fn drop(&mut self) {
        if self.stop_on_drop {
            stop_containers_ecs(self.infra_path.clone())
        }
    }
}

/// Starts the contains described by {infra_path}/docker-compose.yml
/// Returns a hash map of the service names mapped to their respective ip addresses
pub fn start_containers(
    project_name: &'static str,
    infra_path: PathBuf,
    stop_on_drop: bool,
    timeout: Duration,
) -> ContainerInstances {
    let (tx, rx) = channel();
    let _ = thread::spawn(move || {
        tx.send(start_containers_ecs(project_name, infra_path, stop_on_drop))
            .unwrap();
    });

    rx.recv_timeout(timeout).unwrap()
}

fn start_containers_ecs(
    project_name: &str,
    infra_path: PathBuf,
    stop_on_drop: bool,
) -> ContainerInstances {
    let status = Command::new("ecs-cli")
        .args(&["compose", "up", "--create-log-groups"])
        .env("COMPOSE_PROJECT_NAME", project_name)
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
            "ecs-cli compose ps | grep -E '(PROVISIONING|PENDING|RUNNING)' | awk '{print $1 \" \" $3}'",
        ])
        .env("COMPOSE_PROJECT_NAME", project_name)
        .current_dir(infra_path.clone())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
        .wait_with_output()
        .unwrap();

    let instances = String::from_utf8_lossy(&tasks.stdout[..])
        .lines()
        .map(|i| i.split(' ').map(|i| i.to_owned()).collect::<Vec<String>>())
        .map(|i| {
            (
                parse_task_name(i[0].clone()),
                parse_service_port(i[1].clone()),
            )
        })
        .map(|((cluster, task_id, service), port)| {
            let ip_addr = get_task_ip(cluster, task_id.clone());
            if let Some(port) = port {
                println!("Waiting for {service} service to come online");
                wait_for_port_open(ip_addr, port);
            }
            (service, Instance::new(task_id, ip_addr))
        })
        .collect::<HashMap<String, Instance>>();

    ContainerInstances {
        instances,
        infra_path,
        stop_on_drop,
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

fn get_task_ip(cluster: String, task_id: String) -> IpAddr {
    let cmd = if env::var("ANSILO_TESTS_ECS_USE_PUBLIC_IP").is_ok() {
        format!("
            ENI_ID=$(aws ecs describe-tasks --tasks {} --cluster {cluster} --query 'tasks[0].attachments[0].details[?name==`networkInterfaceId`].value' --output text); 
            aws ec2 describe-network-interfaces --network-interface-ids $ENI_ID --query 'NetworkInterfaces[0].Association.PublicIp' --output text
            ", task_id.clone())
    } else {
        format!("aws ecs describe-tasks --tasks {} --cluster {cluster} --query 'tasks[0].attachments[0].details[?name==`privateIPv4Address`].value' --output text", task_id.clone())
    };

    let output = Command::new("bash")
        .args(&["-c".to_string(), cmd])
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

pub fn wait_for_port_open(ip_addr: IpAddr, port: u16) {
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

/// Waits until a specified logging output is seen for the supplied task
pub fn wait_for_log(
    infra_path: PathBuf,
    instance: &Instance,
    log_str: &str,
    timeout: Duration,
) -> () {
    let log_str = log_str.to_string();
    let start_time = std::time::Instant::now();
    let mut child = wait_log_string_command_ecs(infra_path, instance, log_str.clone())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    loop {
        println!("Waiting for {} to appear in task output", log_str.clone());

        if Instant::now() - start_time > timeout {
            panic!("Timedout while waiting for log string {}", log_str.clone());
        }

        match child.try_wait() {
            Ok(Some(_)) => {
                println!("Log string detected!");
                return;
            }
            Ok(None) => {
                thread::sleep(Duration::from_secs(5));
            }
            res @ Err(_) => {
                res.unwrap();
            }
        }
    }
}

fn wait_log_string_command_ecs(
    infra_path: PathBuf,
    instance: &Instance,
    log_str: String,
) -> Command {
    let mut cmd = Command::new("bash");
    cmd.args(&[
        "-c".to_string(),
        format!(
            "grep -m1 -qe '{}' <(ecs-cli logs --follow --task-id {})",
            log_str, instance.id
        ),
    ])
    .current_dir(infra_path.clone());

    cmd
}

/// Gets the current cargo target directory
pub fn get_current_target_dir() -> PathBuf {
    env::current_exe()
        .and_then(|mut p| {
            while p.parent().unwrap().file_name().unwrap().to_string_lossy() != "target" {
                p = p.parent().unwrap().to_path_buf();
            }

            Ok(p)
        })
        .unwrap()
}
