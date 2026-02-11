use ai_detector::{EmailDataset, Emails};
use plotters::prelude::*;
use std::path::Path;

// TODO:
// implement NCD
// some kind of benchmark
// implement LZJD later
// allow users to set datasets
// allow users to set k value
// allow user to set features

fn main() {
    //create a trait and implment the following
    let mut real_enron_emails = EmailDataset::new();
    let mut ai_enron_emails = EmailDataset::new();

    let input_email = "Subject: RE: RE: RE: Project Synergy - Q4 Deliverables

Gary,

Looked at the deck. The \"Web 2.0\" integration looks thin. We need more \"pop\" on the landing page—maybe some high-res gradients?

I’m in the back of a town car right now, signal is spotty. Let’s circle back and touch base during the 8:00 AM status call tomorrow. Don’t forget to CC Brenda.

Best,

Rick

Sent from my BlackBerry® wireless device".to_string();

    let root_area = BitMapBackend::new("chart.png", (1680, 1050)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();

    let mut ctx = ChartBuilder::on(&root_area)
        .set_label_area_size(LabelAreaPosition::Left, 100)
        .set_label_area_size(LabelAreaPosition::Bottom, 100)
        .caption("Real ▲ vs AI o", ("sans-serif", 60))
        .build_cartesian_2d(0.0..1.0, 0.0..1.2)
        .unwrap();

    let original_style = ShapeStyle {
        color: GREEN.mix(0.6),
        filled: true,
        stroke_width: 3,
    };

    ctx.configure_mesh().draw().unwrap();

    real_enron_emails
        .generate_features(Path::new("enron_data/train0.parquet"))
        .unwrap();

    ai_enron_emails
        .generate_features(Path::new("ai_emails.csv"))
        .unwrap();

    let emails = Emails::new(real_enron_emails, ai_enron_emails, input_email).unwrap();
    emails.analyse().unwrap();

    ctx.draw_series(emails.real_emails.features_map.iter().map(|point| {
        TriangleMarker::new(
            (point.1.1.vocab_richness, point.1.1.compression_ratio),
            12,
            &BLUE,
        )
    }))
    .unwrap();

    ctx.draw_series(emails.ai_emails.features_map.iter().map(|point| {
        Circle::new(
            (point.1.1.vocab_richness, point.1.1.compression_ratio),
            12,
            &RED,
        )
    }))
    .unwrap();

    ctx.draw_series(emails.input_email.features_map.iter().map(|point| {
        Circle::new(
            (point.1.1.vocab_richness, point.1.1.compression_ratio),
            15,
            ShapeStyle::filled(&original_style),
        )
    }))
    .unwrap();

    //TODO: add input email to dataset
}
