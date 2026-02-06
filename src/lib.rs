use std::{collections::HashMap, path::Path};

use polars::{
    LazyFrame,
    prelude::{LazyFrame, ScanArgsParquet},
};

use anyhow::{Ok, Result};

pub struct EmailDataset {
    features_map: HashMap<String, Features>,
}

struct Features {
    CompressionRatio: CompressionRatio,
    AverageSentenceLenght: f64,
    VocabRichness: f64,
    SentenceLenghtVariance: f64,
}

type CompressionRatio = f64;
type AverageSentenceLenght = f64;
type VocabRichness = f64;
type SentenceLenghtVariance = f64;

impl EmailDataset {
    pub fn new() -> EmailDataset {
        EmailDataset {
            features_map: HashMap::new(),
        }
    }

    pub fn generate_features(&self, email_dataset_path: &Path) -> Result {
        self.get_trimmed_email_bodies(email_dataset)?;
        self.calculate_features()?;
        Ok(())
    }

    fn get_trimmed_email_bodies(&self, email_dataset_path: &Path) -> Result {
        let lazy_frame = LazyFrame::scan_parquet(email_dataset, ScanArgsParquet::default())?;
        let dataframe = lazy_frame.select(col("body")).limit(100).collect()?;

        let res: Vec<String> = dataframe
            .column("body")?
            .str()?
            .into_iter()
            .map(|val| val.unwrap_or_default().to_owned())?;

        Ok(())

        //use polars to make call to get body
        //use email lib to trim body
    }

    fn calculate_features(&self) -> Result {
        self.generate_compression_ratios();
        self.average_sentence_length();
        self.vocabulary_richness();
        self.sentence_length_variance();
        Ok(())
    }

    fn generate_compression_ratios(&self) {}
    fn average_sentence_length(&self) {}
    fn vocabulary_richness(&self) {}
    fn sentence_length_variance(&self) {}
}
