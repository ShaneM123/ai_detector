use ai_detector::{EmailDataset, Emails};
use std::path::Path;
use tokio::signal;
use tracing::info;

mod handler;
mod homepage;
mod server;
mod shutdown;

// TODO:
// some kind of benchmark
// implement LZJD later
// allow users to set datasets
// allow users to set k value
// allow user to set features

#[derive(Debug, Clone)]
struct Config {
    server_address: String,
    emails: Emails,
}
impl Config {
    pub fn new(server_address: String, emails: Emails) -> Config {
        Config {
            server_address,
            emails,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _trace: Result<(), Box<dyn std::error::Error + Send + Sync>> =
        tracing_subscriber::fmt::try_init();

    let emails = tokio::task::spawn_blocking(|| {
        let mut real_enron_emails = EmailDataset::new();
        let mut ai_enron_emails: EmailDataset = EmailDataset::new();
        real_enron_emails
            .generate_features(Path::new("enron_data/train0.parquet"))
            .unwrap();
        ai_enron_emails
            .generate_features(Path::new("ai_emails.csv"))
            .unwrap();
        Emails::new(real_enron_emails, ai_enron_emails, None).unwrap()
    })
    .await
    .unwrap();
    println!("emails obtained");

    let server_config = Config::new("127.0.0.1:8080".to_string(), emails);
    info!("starting server");

    server::run(server_config, signal::ctrl_c()).await?;

    Ok(())
}
