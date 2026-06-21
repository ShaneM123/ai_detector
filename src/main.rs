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
    let real_emails = env::var("REAL_EMAILS")?;
    let ai_emails = env::var("AI_EMAILS")?;
    let server_address = env::var("SERVER_ADDRESS")?;

    let _trace: Result<(), Box<dyn std::error::Error + Send + Sync>> =
        tracing_subscriber::fmt::try_init();

    let emails = tokio::task::spawn_blocking(move || {
        let mut real_human_emails: EmailDataset = EmailDataset::new();
        let mut ai_faux_emails: EmailDataset = EmailDataset::new();
        real_human_emails
            .generate_features(Path::new(&real_emails))
            .unwrap();
        ai_faux_emails
            .generate_features(Path::new(&ai_emails))
            .unwrap();
        Emails::new(real_human_emails, ai_faux_emails).unwrap()
    })
    .await
    .unwrap();
    println!("emails obtained");

    let server_config = Config::new(server_address, server_cert, server_key, emails, origin);
    info!("starting server");

    server::run(server_config, signal::ctrl_c()).await?;

    Ok(())
}
