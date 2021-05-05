
use std::error::Error;
use std::thread;
use std::time::Duration;
use chrono::{Local};
use lazy_static::lazy_static;
use std::ffi::CString;
use std::env;
use std::collections::HashMap;
use std::path::PathBuf;
use std::fs;
use std::ffi::{OsString, OsStr};
use std::str::FromStr;

use serde::Serialize;


fn parse<S: FromStr, E>(input: Result<String, E>, default: S) -> S
{
    let mut result = default;
    if let Ok(val) = input {
        if let Ok(update) = S::from_str(&val) {
            result = update;
        } else {
            eprintln!("Error unpacking {}", val);
        }
    } else {
        panic!("Error unpacking");
    }
    result
}


struct Environment
{
    host: String,
    mqtt_user: String,
    mqtt_password: String,

    mqtt_host: String,
    mqtt_port: i32,

    topic: String,
    qos: i32,
    interval: f32,
    ca_cert: PathBuf,

    client_cert: Option<PathBuf>,
    client_cert_key: Option<PathBuf>,
    client_cert_key_pass: Option<String>
}

impl Environment
{
    fn new() -> Environment
    {
        let host = env::var("HOSTNAME")
            .expect("Could not read HOSTNAME");
        let mqtt_user = env::var("MQTT_USER")
            .expect("Could not read MQTT_USER");
        let mqtt_password = env::var("MQTT_PASSWORD")
            .expect("MQTT_PASSWORD");
        let mqtt_host = env::var("MQTT_HOST")
            .expect("MQTT_HOST");
        let mqtt_port = parse(env::var("MQTT_PORT"), 8883i32);
        let topic = env::var("MQTT_TOPIC")
            .expect("MQTT_TOPIC");
        let qos = parse(env::var("MQTT_QOS"), 1i32);
        let interval = parse(env::var("MQTT_READ_INTERVAL"), 10.0f32);
        let ca_cert = env::var("CA_CERT").map(PathBuf::from)
            .expect("Could not read CA_CERT");
        let client_cert = env::var("CLIENT_CERT").map(PathBuf::from)
            .ok();
        let client_cert_key = env::var("CLIENT_CERT_KEY").map(PathBuf::from)
            .ok();
        let client_cert_key_pass = env::var("CLIENT_CERT_KEY_PASS")
            .ok();

        Environment {
            host,
            mqtt_user,
            mqtt_password,
            mqtt_host,
            mqtt_port,
            topic,
            qos,
            interval,
            ca_cert,
            client_cert,
            client_cert_key,
            client_cert_key_pass
        }
    }
}

lazy_static! {
    static ref ENVIRONMENT: Environment = Environment::new();
    static ref DS18B20_DEVICE_PATH: PathBuf = PathBuf::from("/sys/bus/w1/devices/");
}

fn get_client() -> Result<paho_mqtt::Client, Box<dyn Error>>
{
    eprintln!("Setting up client options");
    let options = paho_mqtt::CreateOptionsBuilder::new()
        .server_uri(format!("tcp://{}:{}", &ENVIRONMENT.host, &ENVIRONMENT.mqtt_port))
        .client_id(&ENVIRONMENT.host)
        .finalize();

    eprintln!("Creating client");
    let client = paho_mqtt::Client::new(options)?;

    eprintln!("Setting up SSL options");
    let mut ssl_options_builder = paho_mqtt::SslOptionsBuilder::new();

    ssl_options_builder.ssl_version(paho_mqtt::SslVersion::Tls_1_2)
        .ca_path(&ENVIRONMENT.ca_cert)?;

    if let Some(ref client_cert) = &ENVIRONMENT.client_cert {
        if let Some(ref client_key) = &ENVIRONMENT.client_cert_key {
            if let Some(ref client_key_pass) = &ENVIRONMENT.client_cert_key_pass {
                ssl_options_builder.key_store(&ENVIRONMENT.client_cert)?
                    .private_key(&ENVIRONMENT.client_cert_key)?
                    .private_key_password(&ENVIRONMENT.client_cert_key_pass);
            }
        }
    }

    let ssl_options = ssl_options_builder.finalize();

    eprintln!("Creating connect options");

    let connect_options = paho_mqtt::ConnectOptionsBuilder::new()
        .user_name(&ENVIRONMENT.mqtt_user)
        .password(&ENVIRONMENT.mqtt_password)
        .ssl_options(ssl_options)
        .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(10))
        .retry_interval(Duration::from_secs(5))
        .finalize();

    eprintln!("Connecting");

    client.connect(
        connect_options
    ).unwrap();

    eprintln!("Connected");

    Ok(client)
}

trait Sensor
{
    fn identifier(&self) -> &str;
    fn read_to_string(&self) -> String;
}

struct DS18B20Sensor
{
    id: String
}

#[derive(Serialize)]
struct DS18B20Reading
{
    temperature: f32
}

impl DS18B20Reading
{
    fn new(temperature: f32) -> DS18B20Reading
    {
        DS18B20Reading { temperature }
    }
}

impl DS18B20Sensor {
    fn new(id_os: &str) -> DS18B20Sensor
    {
        DS18B20Sensor { id: id_os.into() }
    }
}

impl Sensor for DS18B20Sensor {

    fn identifier(&self) -> &str
    {
         &self.id
    }

    fn read_to_string(&self) -> String
    {
        let path = DS18B20_DEVICE_PATH.join(&self.id).join("w1_slave");

        let string_contents: String = match fs::read(&path) {
            Ok(contents) => {
                String::from_utf8(contents).unwrap_or("".into())
            },
            Err(_) => {
                return serde_json::to_string(&DS18B20Reading::new(f32::NAN)).unwrap();
            }
        };

        // This is a really naive implementation, needs more robustness
        if string_contents.is_empty() {
            return serde_json::to_string(&DS18B20Reading::new(f32::NAN)).unwrap();
        }

        let mut lines = string_contents.lines();

        let line1 = match lines.next() {
            Some(line) => line,
            None => return serde_json::to_string(&DS18B20Reading::new(f32::NAN)).unwrap()
        };

        let line2 = match lines.next() {
            Some(line) => line,
            None => return serde_json::to_string(&DS18B20Reading::new(f32::NAN)).unwrap()
        };

        if !line1.ends_with("YES") {
            return serde_json::to_string(&DS18B20Reading::new(f32::NAN)).unwrap()
        }

        let itemp: i32 = match line2.rsplit('=').next().map(i32::from_str) {
            Some(Ok(v)) => v,
            _ => return serde_json::to_string(&DS18B20Reading::new(f32::NAN)).unwrap()
        };


        let reading = DS18B20Reading::new((itemp as f32) / 1000.0f32);
        serde_json::to_string(&reading).unwrap()
    }
}

fn get_sensors() -> Result<Vec<Box<dyn Sensor>>, Box<dyn Error> >
{
    let mut result: Vec<Box<dyn Sensor>> = Vec::new();

    for device in fs::read_dir(&DS18B20_DEVICE_PATH.as_path())? {
        if let Ok(dev) = device {
            let path = dev.path();
            let id = path
                .strip_prefix(DS18B20_DEVICE_PATH.as_path())
                .unwrap().as_os_str().to_string_lossy();
            if !id.starts_with("28-") {
                 continue;
            }

            result.push(Box::new(DS18B20Sensor::new(&id)));
        }
    }

    // Get the AM2320 sensor, which is a bit more complicated.

    Ok(result)
}



fn main() -> Result<(), Box<dyn Error>>
{
    let client = get_client()?;

    let wait_time = Duration::from_secs_f32(ENVIRONMENT.interval);

    let sensors = get_sensors()?;

    loop {
        thread::sleep(wait_time);

        let mut readings = HashMap::new();

        for sensor in &sensors {
            readings.insert(sensor.identifier(), sensor.read_to_string());
        }

        let message = paho_mqtt::Message::new(
            &ENVIRONMENT.topic,
            serde_json::to_string(&readings).unwrap_or("ERR".into()),
            ENVIRONMENT.qos
        );

        if !client.is_connected() {
            if let Err(_e) = client.reconnect() {
                continue;
            }
        }

        if let Err(e) = client.publish(message) {
            eprintln!("An error occurred publishing message {:?}", e);
        }

    }

    Ok(())
}




