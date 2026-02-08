use std::{
    collections::{HashMap, HashSet},
    io::Write,
    path::Path,
    sync::Arc,
};

use polars::prelude::{LazyFrame, PlPath, ScanArgsParquet, avg, col};

use anyhow::{Ok, Result as AnyhowResult, anyhow};
use flate2::{Compression, write::GzEncoder};
use mail_parser::MessageParser;

#[derive(Debug)]
pub struct EmailDataset {
    pub features_map: HashMap<CompressedEmailVec, Features>,
    email_bodies: Vec<String>,
}
type CompressedEmailVec = Vec<u8>;

#[derive(Debug)]
pub struct Features {
    pub compression_ratio: CompressionRatio,
    pub average_sentence_length: AverageSentenceLength,
    pub vocab_richness: VocabRichness,
    pub sentence_length_variance: SentenceLenghtVariance,
}

type CompressionRatio = f64;
type AverageSentenceLength = f64;
type VocabRichness = f64;
type SentenceLenghtVariance = f64;

impl EmailDataset {
    pub fn new() -> EmailDataset {
        EmailDataset {
            features_map: HashMap::new(),
            email_bodies: Vec::new(),
        }
    }

    pub fn generate_features(&mut self, email_dataset_path: &Path) -> AnyhowResult<()> {
        self.get_trimmed_email_bodies(email_dataset_path)?;
        self.calculate_features()?;
        Ok(())
    }

    fn get_trimmed_email_bodies(&mut self, email_dataset_path: &Path) -> AnyhowResult<()> {
        let lazy_frame: LazyFrame = LazyFrame::scan_parquet(
            PlPath::Local(Arc::from(email_dataset_path)),
            ScanArgsParquet::default(),
        )?;
        let dataframe = lazy_frame.select([col("body")]).limit(10).collect()?;

        self.email_bodies = dataframe
            .column("body")?
            .str()?
            .into_iter()
            .map(|val| val.unwrap_or_default().to_owned())
            .collect();

        //println!("emails:{:?} ", self.email_bodies);

        //self.tidy_email_bodies()?;

        //println!("emails:{:?}", self.email_bodies.get(15));

        Ok(())
    }

    fn _tidy_email_bodies(&mut self) -> AnyhowResult<()> {
        for input in self.email_bodies.iter_mut() {
            let message = MessageParser::default().parse(&input).unwrap();
            *input = message
                .body_text(0)
                .ok_or(anyhow!("error getting body text from message"))?
                .to_string();
        }

        Ok(())
    }

    fn calculate_features(&mut self) -> AnyhowResult<()> {
        for email in &self.email_bodies {
            // -- calculate compression ratio --
            let orginal_size = email.len() as f64;
            let mut encoder: GzEncoder<Vec<u8>> =
                GzEncoder::new(Vec::new(), Compression::default());
            let _res = encoder.write_all(email.as_bytes());
            let compressed_email = encoder.finish()?;
            let compression_size = compressed_email.len() as f64;
            let compression_ratio: CompressionRatio = 1.00 - compression_size / orginal_size;

            // -- calculate average sentence length --

            let sentence_accum = email
                .split_terminator(|c: char| c == '.' || c == '!' || c == '?')
                .filter(|s| !s.trim().is_empty())
                .fold((0.0, 0.0), |(count, total), val| {
                    (count + 1.0, total + (val.len() as f64))
                });

            let avg = sentence_accum.1 / sentence_accum.0;

            // -- calculate vocab richness --

            let words = email.split_ascii_whitespace();

            let word_count = words.clone().count() as f64;

            let unique_word_count = words.into_iter().collect::<HashSet<&str>>().len() as f64;

            let vocab_richness = unique_word_count / word_count;

            // -- sentence length variance --

            let sentences = email
                .split_terminator(|c: char| c == '.' || c == '!' || c == '?')
                .filter(|s| !s.trim().is_empty())
                .collect::<Vec<&str>>();

            let sentence_word_counts: Vec<f64> = sentences
                .iter()
                .map(|x| x.chars().count() as f64)
                .collect::<Vec<f64>>();

            let sentence_count = sentences.len() as f64;
            let mean = word_count / sentence_count;

            let squared_sum = sentence_word_counts
                .iter()
                .fold(0.0, |accum, val| accum + (val - mean).powf(2.0));

            let sentence_variance = squared_sum / sentence_count - 1.0;

            self.features_map.insert(
                compressed_email,
                Features {
                    compression_ratio,
                    average_sentence_length: avg,
                    vocab_richness: vocab_richness,
                    sentence_length_variance: sentence_variance,
                },
            );
        }
        Ok(())
    }
}
