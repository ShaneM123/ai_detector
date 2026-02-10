use ai_detector::{EmailDataset, Emails};
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

    let input_email =
        "Subject: Out of Office: Jordan Lee – Wednesday, Feb 11

Hi Sarah,

Please note that I will be out of the office tomorrow, Wednesday, February 11, to attend to some scheduled personal administrative matters.

To ensure a smooth workflow in my absence:

    Daily Tasks: I’ve moved my recurring morning reports to Thursday morning.

    Support: For any immediate technical needs, please direct the team to Chris Miller, who has the login credentials for the shared dashboard.

    Communications: My Slack status is updated, and my OOO reply will direct people to the appropriate departments.

I will be back online and caught up by 9:00 AM on Thursday. Thanks for your support!

Best regards,

Jordan Lee Technical Specialist"
            .to_string();

    real_enron_emails
        .generate_features(Path::new("enron_data/train0.parquet"))
        .unwrap();

    ai_enron_emails
        .generate_features(Path::new("ai_emails.csv"))
        .unwrap();

    let emails = Emails::new(real_enron_emails, ai_enron_emails, input_email).unwrap();

    emails.analyse().unwrap();

    //TODO: add input email to dataset
}
