use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    io::Write,
    path::Path,
    sync::Arc,
};

use polars::prelude::{LazyFrame, PlPath, ScanArgsParquet, col};

use anyhow::{Ok, Result as AnyhowResult, anyhow};
use csv::ReaderBuilder;
use flate2::{Compression, write::GzEncoder};
use mail_parser::MessageParser;

#[derive(Debug)]
pub struct EmailDataset {
    pub features_map: HashMap<CompressedEmailVec, Features>,
    pub email_bodies: Vec<String>,
}
type CompressedEmailVec = Vec<u8>;

#[derive(Debug, Clone, Copy)]
pub struct Features {
    pub compression_ratio: CompressionRatio,
    pub average_sentence_length: AverageSentenceLength,
    pub vocab_richness: VocabRichness,
    pub sentence_length_variance: SentenceLenghtVariance,
}

impl IntoIterator for Features {
    type Item = f64;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    // Required method
    fn into_iter(self) -> Self::IntoIter {
        vec![
            self.average_sentence_length,
            self.compression_ratio,
            self.sentence_length_variance,
            self.vocab_richness,
        ]
        .into_iter()
    }
}

type CompressionRatio = f64;
type AverageSentenceLength = f64;
type VocabRichness = f64;
type SentenceLenghtVariance = f64;

pub struct Emails {
    real_emails: EmailDataset,
    ai_emails: EmailDataset,
    input_email: EmailDataset,
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
            .insert(input_features.0, input_features.1);
        input_dataset.email_bodies.push(input_email);

        Ok(Emails {
            real_emails,
            ai_emails,
            input_email: input_dataset,
        })
    }

    pub fn analyse(&self) -> AnyhowResult<()> {
        //calculate distances
        for input_email in self.input_email.features_map.iter() {
            let mut distances = Vec::new();

            //TODO: fix iteration, was wrong in the first place, maye have to move the uncompressed value to the hash
            for ai_email in self.ai_emails.features_map.iter() {
                distances.push((true, Self::elucidian_distance(*input_email.1, *ai_email.1)));
            }
            for real_email in self.real_emails.features_map.iter() {
                distances.push((
                    false,
                    Self::elucidian_distance(*input_email.1, *real_email.1),
                ));
            }

            distances.sort_by(|a, b| cmp_f64(&a.1, &b.1));

            //take 7 closest and find majority, true's = ai, false's = real
            let total_true = distances.iter().take(7).filter(|x| x.0).count();
            if total_true < 4 {
                print!("Its a real email");
            } else {
                println!("It's written by ai");
            }
        }
        Ok(())
    }

    fn elucidian_distance(features_one: Features, features_two: Features) -> f64 {
        features_one
            .into_iter()
            .zip(features_two.into_iter())
            .fold(0.0, |accum, features: (f64, f64)| {
                accum + (features.0 - features.1).powf(2.0)
            })
            .sqrt()
    }

    // f1 = sys.argv[1]
    // f2 = sys.argv[2]
    // fd1=open(f1,"rb")
    // x=fd1.read()
    // fd1.close()
    // fd2=open(f2,"rb")
    // y=fd2.read()
    // fd2.close()
    // xy=x+y
    // zxy = lzma.compress(xy)
    // zx = lzma.compress(x)
    // zy = lzma.compress(y)
    // print "Length of compressed concatination: %d"%len(zxy)
    // print "Length of compressed x: %d"%len(zx)
    // print "Length of compressed y: %d"%len(zy)
    // ncd = ((len(zxy)-min(len(zx), len(zy)))/(max(len(zx), len(zy))))
    // print "{} {}".format(sys.argv[2],ncd)

    fn ncd(features_one: Features, features_two: Features) -> f64 {
        // add one and two to get onetwo
        // compress, one, two and onetwo respectively
        // calculate ncd as such:
        // ncd = ((len(comp_onetwo)-min(comp_one.len(), comp_two.len()))/(max(comp_one, comp_two))))

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
            self.features_map.insert(features.0, features.1);
        }
        Ok(())
    }
}

pub fn calculate_features(email: &String) -> AnyhowResult<(Vec<u8>, Features)> {
    // -- calculate compression ratio --
    let orginal_size = email.len() as f64;
    let mut encoder: GzEncoder<Vec<u8>> = GzEncoder::new(Vec::new(), Compression::default());
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

    Ok((
        compressed_email,
        Features {
            compression_ratio,
            average_sentence_length: avg,
            vocab_richness: vocab_richness,
            sentence_length_variance: sentence_variance,
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
