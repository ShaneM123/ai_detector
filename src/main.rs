use ai_detector::{EmailDataset, Emails};
use std::env;
use std::path::Path;
use tokio::signal;
use tracing::info;

mod handler;
mod homepage;
mod server;
mod shutdown;

#[derive(Debug, Clone)]
struct Config {
    server_address: String,
    server_cert: String,
    server_key: String,
    emails: Emails,
    origin: String,
}
impl Config {
    pub fn new(
        server_address: String,
        server_cert: String,
        server_key: String,
        emails: Emails,
        origin: String,
    ) -> Config {
        Config {
            server_address,
            server_cert,
            server_key,
            emails,
            origin,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("launching...");
    dotenv::from_filename("./local.env")?;
    println!("getting certs");
    let server_cert = env::var("SERVER_CERT")?;
    let server_key = env::var("SERVER_KEY")?;
    let origin = env::var("ORIGIN")?;

    let _trace: Result<(), Box<dyn std::error::Error + Send + Sync>> =
        tracing_subscriber::fmt::try_init();

    let emails = tokio::task::spawn_blocking(|| {
        let mut real_enron_emails: EmailDataset = EmailDataset::new();
        let mut ai_enron_emails: EmailDataset = EmailDataset::new();
        real_enron_emails
            .generate_features(Path::new("/enron_data/train0.parquet"))
            .unwrap();
        ai_enron_emails
            .generate_features(Path::new("/ai_emails/ai_emails.csv"))
            .unwrap();
        Emails::new(real_enron_emails, ai_enron_emails).unwrap()
    })
    .await
    .unwrap();
    println!("emails obtained");

    let server_config = Config::new(
        "0.0.0.0:8086".to_string(),
        server_cert,
        server_key,
        emails,
        origin,
    );
    info!("starting server");

    server::run(server_config, signal::ctrl_c()).await?;

    Ok(())
}
