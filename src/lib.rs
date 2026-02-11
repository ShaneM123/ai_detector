use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use polars::prelude::{LazyFrame, PlPath, ScanArgsParquet, col};

use anyhow::{Ok, Result as AnyhowResult, anyhow};
use csv::ReaderBuilder;
use flate2::{Compress, Compression};
use mail_parser::MessageParser;

#[derive(Debug)]
pub struct EmailDataset {
    pub features_map: HashMap<CompressedEmailVec, (String, Features)>,
    pub email_bodies: Vec<String>,
}
type CompressedEmailVec = Vec<u8>;

#[derive(Debug, Clone, Copy)]
pub struct Features {
    pub compression_length: CompressionLength,
    pub average_sentence_length: AverageSentenceLength,
    pub vocab_richness: VocabRichness,
    pub sentence_length_variance: SentenceLenghtVariance,
    pub compression_ratio: CompressionRatio,
}

impl IntoIterator for Features {
    type Item = f64;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    // Required method
    fn into_iter(self) -> Self::IntoIter {
        vec![
            self.average_sentence_length,
            self.compression_length,
            self.sentence_length_variance,
            self.vocab_richness,
        ]
        .into_iter()
    }
}

type CompressionLength = f64;
type AverageSentenceLength = f64;
type VocabRichness = f64;
type SentenceLenghtVariance = f64;
type CompressionRatio = f64;
pub struct Emails {
    pub real_emails: EmailDataset,
    pub ai_emails: EmailDataset,
    pub input_email: EmailDataset,
}
impl Emails {
    pub fn new(
        real_emails: EmailDataset,
        ai_emails: EmailDataset,
        input_email: String,
    ) -> AnyhowResult<Emails> {
        let mut input_dataset = EmailDataset::new();
        let input_features = calculate_features(&input_email)?;
        input_dataset
            .features_map
            .insert(input_features.0, (input_email.clone(), input_features.1));
        input_dataset.email_bodies.push(input_email);

        Ok(Emails {
            real_emails,
            ai_emails,
            input_email: input_dataset,
        })
    }

    //TODO: figure out why ncd is not working
    pub fn analyse(&self) -> AnyhowResult<()> {
        //calculate distances
        for input_email in self.input_email.features_map.iter() {
            let mut distances = Vec::new();

            //TODO: fix iteration, was wrong in the first place, maye have to move the uncompressed value to the hash
            //TODO: improve performance by removing clones
            //TODO: figure out if its messing up with the other features a bit, as they are ratios
            for ai_email in self.ai_emails.features_map.iter() {
                distances.push((
                    true,
                    [
                        //Self::ncd(input_email.1.clone(), ai_email.1.clone())?,
                        Self::elucidian_distance(
                            vec![
                                input_email.1.1.compression_ratio,
                                input_email.1.1.vocab_richness,
                            ],
                            vec![ai_email.1.1.compression_ratio, ai_email.1.1.vocab_richness],
                        ),
                        //ai_email.1.1.average_sentence_length,
                        // ai_email.1.1.sentence_length_variance,
                        //ai_email.1.1.vocab_richness,
                    ],
                ));
            }
            for real_email in self.real_emails.features_map.iter() {
                distances.push((
                    false,
                    [
                        //Self::ncd(input_email.1.clone(), real_email.1.clone())?,
                        Self::elucidian_distance(
                            vec![
                                input_email.1.1.compression_ratio,
                                input_email.1.1.vocab_richness,
                            ],
                            vec![
                                real_email.1.1.compression_ratio,
                                real_email.1.1.vocab_richness,
                            ],
                        ),
                        //real_email.1.1.average_sentence_length,
                        // real_email.1.1.sentence_length_variance,
                        //real_email.1.1.vocab_richness,
                    ],
                ));
            }

            distances.sort_by(|a, b| cmp_f64(&a.1.iter().sum(), &b.1.iter().sum()));
            println!("DISTANCES: {:?}", distances);

            //take 7 closest and find majority, true's = ai, false's = real
            let total_true = distances.iter().take(5).filter(|x| x.0).count();
            if total_true < 4 {
                println!("Its a real email");
            } else {
                println!("It's written by ai");
            }
        }
        Ok(())
    }

    fn ncd(
        features_one: (String, Features),
        features_two: (String, Features),
    ) -> AnyhowResult<f64> {
        let mut compressor = Compress::new(Compression::default(), false);

        compressor.compress(
            features_two
                .0
                .as_bytes()
                .into_iter()
                .chain(features_one.0.as_bytes().into_iter())
                .copied()
                .into_iter()
                .collect::<Vec<u8>>()
                .as_slice(),
            &mut vec![0; 1024],
            flate2::FlushCompress::Finish,
        )?;
        let combined_length = compressor.total_out() as f64;

        // ncd = ((len(xy)-min(len(x),(y)))/(max(len(x), len(y)))))
        Ok((combined_length
            - f64::min(
                features_one.1.compression_length,
                features_one.1.compression_length,
            ))
            / (f64::max(
                features_one.1.compression_length,
                features_one.1.compression_length,
            )))
    }

    //not needed when using ncd instead
    fn elucidian_distance(features_one: Vec<f64>, features_two: Vec<f64>) -> f64 {
        features_one
            .into_iter()
            .zip(features_two.into_iter())
            .fold(0.0, |accum, features: (f64, f64)| {
                accum + (features.0 - features.1).powf(2.0)
            })
            .sqrt()
    }
}

impl EmailDataset {
    pub fn new() -> EmailDataset {
        EmailDataset {
            features_map: HashMap::new(),
            email_bodies: Vec::new(),
        }
    }

    pub fn generate_features(&mut self, email_dataset_path: &Path) -> AnyhowResult<()> {
        self.get_trimmed_email_bodies(email_dataset_path)?;
        self.calculate_dataset_features()?;
        Ok(())
    }

    fn get_trimmed_email_bodies(&mut self, email_dataset_path: &Path) -> AnyhowResult<()> {
        let extension = email_dataset_path
            .extension()
            .ok_or(anyhow!("cant find file extension"))?;
        if extension == "parquet" {
            let lazy_frame: LazyFrame = LazyFrame::scan_parquet(
                PlPath::Local(Arc::from(email_dataset_path)),
                ScanArgsParquet::default(),
            )?;
            let dataframe = lazy_frame.select([col("body")]).limit(50).collect()?;

            self.email_bodies = dataframe
                .column("body")?
                .str()?
                .into_iter()
                .map(|val| val.unwrap_or_default().to_owned())
                .collect();
        } else if extension == "csv" {
            self.email_bodies = ReaderBuilder::new()
                .delimiter(b';')
                .has_headers(false)
                .from_path(email_dataset_path)?
                .records()
                .map(|val| {
                    val.ok()
                        .expect("expected a string record")
                        .as_slice()
                        .to_string()
                })
                .collect::<Vec<String>>();
        } else {
            return Err(anyhow!(
                "expected parquet or csv file, found {:?}",
                extension
            ));
        }
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

    pub fn calculate_dataset_features(&mut self) -> AnyhowResult<()> {
        for email in &self.email_bodies {
            let features = calculate_features(email)?;
            self.features_map
                .insert(features.0, (email.clone(), features.1));
        }
        Ok(())
    }
}

//TODO: skip short emails, harder to analyse
pub fn calculate_features(email: &String) -> AnyhowResult<(Vec<u8>, Features)> {
    // -- calculate compression ratio --
    let mut compressor = Compress::new(Compression::best(), false);

    let mut compressor_output = [0; 1024];

    compressor.compress(
        email.as_bytes(),
        &mut compressor_output,
        flate2::FlushCompress::Finish,
    )?;
    let compression_length = compressor.total_out() as f64;
    let compression_ratio = compression_length / email.as_bytes().len() as f64;
    let compressed_email = compressor_output.to_vec();

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
    println!("word count: {}", word_count);
    println!("unique word count: {}", unique_word_count);
    let vocab_richness = unique_word_count / word_count;
    println!("vocab richness: {}", vocab_richness);

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

    Ok((
        compressed_email,
        Features {
            compression_length: compression_length,
            average_sentence_length: avg,
            vocab_richness: vocab_richness,
            sentence_length_variance: sentence_variance,
            compression_ratio: compression_ratio,
        },
    ))
}

fn cmp_f64(a: &f64, b: &f64) -> Ordering {
    if a.is_nan() {
        return Ordering::Greater;
    }
    if b.is_nan() {
        return Ordering::Less;
    }
    if a < b {
        return Ordering::Less;
    } else if a > b {
        return Ordering::Greater;
    }
    return Ordering::Equal;
}
