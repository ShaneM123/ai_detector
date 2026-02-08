//TODO:
// read about KNN
// implement basic knn
//grab some ai texts
// grab some non ai texts
// train the knn

// get texts collection
// gzip them

// shove em into elucidian with a bunch of neighbours

// features [compression_ratio, sentence_length_variance (or coefficient of variation = std_dev / mean), vocabulary_richness, average_sentence_length]

// do the same for ai

use std::path::Path;

use ai_detector::EmailDataset;

fn main() {
    //create a trait and implment the following
    let mut enron_emails = EmailDataset::new();

    enron_emails
        .generate_features(Path::new("enron_data/train0.parquet"))
        .unwrap();

    println!("dataset: {:?}", enron_emails.features_map);
}
