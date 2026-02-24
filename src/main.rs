use ai_detector::{EmailDataset, Emails};
use plotters::prelude::*;
use std::path::Path;
use tokio::signal;
use tracing::info;

mod connections;
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
// TODO: add input email to dataset

//TODO: single thread load data sets and put them behind a mutex on startup
// accept incoming connections and pass it to analyse, allow user to set k values
// return result and png image of graph
// delete graphs

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
    let trace: Result<(), Box<dyn std::error::Error + Send + Sync>> =
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

    //     let input_email: String = "Hi Shane,

    // Great to hear you enjoyed being with the team. Unfortunately I was not able to get any feedback from the team so far but I`ll have my weekly update with the CTO, Martin Menscher later today. After that I´ll come back to you with further steps. Thank you for your patience.

    // In the meantime, would you send me your bank account details? So I can forward your receipts to our financial admin.

    // Have a sunny day,
    // Romy".to_string();

    //     //TODO: add filters and regex checking, make sure email isnt a pile of shite
    //     emails.set_input(input_email);
    //     emails.analyse().unwrap();

    //     let root_area = BitMapBackend::new("chart.png", (3200, 2080)).into_drawing_area();
    //     root_area.fill(&WHITE).unwrap();

    //     let mut ctx = ChartBuilder::on(&root_area)
    //         .set_label_area_size(LabelAreaPosition::Left, 100)
    //         .set_label_area_size(LabelAreaPosition::Bottom, 100)
    //         .caption("Real ▲ vs AI o", ("sans-serif", 60))
    //         .build_cartesian_2d(0.0..1.2, 0.0..1.2)
    //         .unwrap();

    //     let original_style = ShapeStyle {
    //         color: GREEN.mix(0.6),
    //         filled: true,
    //         stroke_width: 3,
    //     };

    //     ctx.configure_mesh().draw().unwrap();

    //     ctx.draw_series(emails.real_emails.features_map.iter().map(|point| {
    //         TriangleMarker::new(
    //             (point.1.1.vocab_richness, point.1.1.compression_ratio),
    //             12,
    //             &BLUE,
    //         )
    //     }))
    //     .unwrap();

    //     ctx.draw_series(emails.ai_emails.features_map.iter().map(|point| {
    //         Circle::new(
    //             (point.1.1.vocab_richness, point.1.1.compression_ratio),
    //             12,
    //             &RED,
    //         )
    //     }))
    //     .unwrap();

    //     ctx.draw_series(
    //         emails
    //             .input_email
    //             .as_ref()
    //             .expect("no input email found")
    //             .features_map
    //             .iter()
    //             .map(|point| {
    //                 Circle::new(
    //                     (point.1.1.vocab_richness, point.1.1.compression_ratio),
    //                     15,
    //                     ShapeStyle::filled(&original_style),
    //                 )
    //             }),
    //     )
    //     .unwrap();
    Ok(())
}
