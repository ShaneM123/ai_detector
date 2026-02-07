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

pub struct EmailDataset {
    features_map: HashMap<CompressedEmailVec, Features>,
    email_bodies: Vec<String>,
}
type CompressedEmailVec = Vec<u8>;

struct Features {
    compression_ratio: CompressionRatio,
    average_sentence_length: AverageSentenceLength,
    vocab_richness: VocabRichness,
    sentence_length_variance: SentenceLenghtVariance,
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
        let dataframe = lazy_frame.select([col("body")]).limit(100).collect()?;

        self.email_bodies = dataframe
            .column("body")?
            .str()?
            .into_iter()
            .map(|val| val.unwrap_or_default().to_owned())
            .collect();

        println!("emails:{:?} ", self.email_bodies);

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
            let orginal_size = email.len() as f64;
            let mut encoder: GzEncoder<Vec<u8>> =
                GzEncoder::new(Vec::new(), Compression::default());
            let _res = encoder.write_all(email.as_bytes());
            let compressed_email = encoder.finish()?;
            let compression_size = compressed_email.len() as f64;
            let compression_ratio: CompressionRatio = 1.00 - compression_size / orginal_size;

            let sentence_accum = email
                .split_terminator(|c: char| c == '.' || c == '!' || c == '?')
                .filter(|s| !s.trim().is_empty())
                .fold((0.0, 0.0), |(count, total), val| {
                    (count + 1.0, total + (val.len() as f64))
                });

            let avg = sentence_accum.1 / sentence_accum.0;

            let words = email.split_ascii_whitespace();

            let word_count = words.clone().count() as f64;

            let unique_word_count = words.into_iter().collect::<HashSet<&str>>().len() as f64;

            let vocab_richness = unique_word_count / word_count;

            //         let sentence_accum = email
            // .split_terminator(|c: char| c == '.' || c == '!' || c == '?')
            // .filter(|s| !s.trim().is_empty())
            // .fold((0.0, 0.0), |(count, total), val| {
            //     (count + 1.0, total + (val.len() as f64))
            // });

            self.features_map.insert(
                compressed_email,
                Features {
                    compression_ratio,
                    average_sentence_length: avg,
                    vocab_richness: vocab_richness,
                    sentence_length_variance: 0.00,
                },
            );
            // self.generate_compression_ratios(email)?;
            // self.average_sentence_length(email);
            // self.vocabulary_richness();
            // self.sentence_length_variance();
        }
        Ok(())
    }

    fn generate_compression_ratios(&mut self, email: &String) -> AnyhowResult<()> {
        Ok(())
    }
    fn average_sentence_length(&self, email: &String) -> AnyhowResult<()> {
        Ok(())
    }
    fn vocabulary_richness(&self) {}
    fn sentence_length_variance(&self) {}
}
