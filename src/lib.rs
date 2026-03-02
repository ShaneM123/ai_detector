use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    io::Cursor,
    path::Path,
    sync::Arc,
};

use anyhow::{Ok, Result as AnyhowResult, anyhow};
use csv::ReaderBuilder;
use flate2::{Compress, Compression, Status};
use image::{ImageBuffer, Rgb};
use mail_parser::MessageParser;
use plotters::prelude::*;
use polars::prelude::{LazyFrame, PlPath, ScanArgsParquet, col};
use tokio::sync::Mutex;
use tracing::info;

#[derive(Clone)]
pub struct EmailDropGuard {
    pub emails: Arc<tokio::sync::Mutex<Emails>>,
}

impl EmailDropGuard {
    pub fn new(emails: Emails) -> EmailDropGuard {
        EmailDropGuard {
            emails: Arc::new(Mutex::new(emails)),
        }
    }
}

#[derive(Debug, Clone)]
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

#[derive(Clone, Debug)]
pub struct Emails {
    pub real_emails: EmailDataset,
    pub ai_emails: EmailDataset,
    pub input_email: Option<EmailDataset>,
}
impl Emails {
    pub fn new(
        real_emails: EmailDataset,
        ai_emails: EmailDataset,
        input_email: Option<EmailDataset>,
    ) -> AnyhowResult<Emails> {
        Ok(Emails {
            real_emails,
            ai_emails,
            input_email: input_email,
        })
    }

    pub fn set_input(&mut self, input_email: String) -> AnyhowResult<()> {
        let mut input_dataset = EmailDataset::new();
        let input_features = calculate_features(&input_email)?;
        input_dataset
            .features_map
            .insert(input_features.0, (input_email.clone(), input_features.1));
        input_dataset.email_bodies.push(input_email);
        self.input_email = Some(input_dataset);

        Ok(())
    }

    pub fn analyse(&self) -> AnyhowResult<(bool, Vec<u8>)> {
        //calculate distances
        for input_email in self
            .input_email
            .as_ref()
            .expect("no input email")
            .features_map
            .iter()
        {
            let mut distances = Vec::new();

            for ai_email in self.ai_emails.features_map.iter() {
                distances.push((
                    true,
                    [
                        Self::ncd(input_email.1.clone(), ai_email.1.clone())?,
                        Self::elucidian_distance(
                            vec![input_email.1.1.vocab_richness],
                            vec![ai_email.1.1.vocab_richness],
                        ),
                    ],
                ));
            }
            for real_email in self.real_emails.features_map.iter() {
                distances.push((
                    false,
                    [
                        Self::ncd(input_email.1.clone(), real_email.1.clone())?,
                        Self::elucidian_distance(
                            vec![input_email.1.1.vocab_richness],
                            vec![real_email.1.1.vocab_richness],
                        ),
                    ],
                ));
            }

            distances.sort_by(|a, b| cmp_f64(&a.1.iter().sum(), &b.1.iter().sum()));

            let total_true = distances.iter().take(13).filter(|x| x.0).count();
            let image = self.generate_image()?;

            if total_true < 7 {
                info!("Its a real email");
                return Ok((true, image));
            } else {
                info!("It's written by ai");
                return Ok((false, image));
            }
        }
        Err(anyhow!("couldnt analyse email"))
    }
    pub fn ncd(
        features_one: (String, Features),
        features_two: (String, Features),
    ) -> AnyhowResult<f64> {
        let mut combined = features_two.0.as_bytes().to_vec();
        combined.extend_from_slice(features_one.0.as_bytes());

        let (combined_length, _compressed_emails) = compress(&combined)?;

        // ncd = ((len(xy)-min(len(x),(y)))/(max(len(x), len(y)))))
        Ok((combined_length
            - f64::min(
                features_one.1.compression_length,
                features_two.1.compression_length,
            ))
            / (f64::max(
                features_one.1.compression_length,
                features_two.1.compression_length,
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

    fn generate_image(&self) -> AnyhowResult<Vec<u8>> {
        let width = 3200;
        let height = 2080;
        let mut buffer = vec![0u8; (width * height * 3) as usize];
        {
            let root_area: DrawingArea<BitMapBackend<'_>, plotters::coord::Shift> =
                BitMapBackend::with_buffer_and_format(&mut buffer, (3200, 2080))?
                    .into_drawing_area();
            root_area.fill(&WHITE)?;

            let mut ctx: ChartContext<
                '_,
                BitMapBackend<'_>,
                Cartesian2d<
                    plotters::coord::types::RangedCoordf64,
                    plotters::coord::types::RangedCoordf64,
                >,
            > = ChartBuilder::on(&root_area)
                .set_label_area_size(LabelAreaPosition::Left, 100)
                .set_label_area_size(LabelAreaPosition::Bottom, 100)
                .caption(
                    "Real Emails (Triangle) vs AI Emails (O's)",
                    ("sans-serif", 80),
                )
                .build_cartesian_2d(0.0..1.2, 0.0..1.2)?;

            let original_style = ShapeStyle {
                color: GREEN.mix(0.8),
                filled: true,
                stroke_width: 4,
            };

            ctx.configure_mesh().draw()?;

            ctx.draw_series(self.real_emails.features_map.iter().map(|point| {
                TriangleMarker::new(
                    (point.1.1.vocab_richness, point.1.1.compression_ratio),
                    12,
                    &BLUE,
                )
            }))?;

            ctx.draw_series(self.ai_emails.features_map.iter().map(|point| {
                Circle::new(
                    (point.1.1.vocab_richness, point.1.1.compression_ratio),
                    12,
                    &RED,
                )
            }))?;

            ctx.draw_series(
                self.input_email
                    .as_ref()
                    .expect("no input email found")
                    .features_map
                    .iter()
                    .map(|point| {
                        Circle::new(
                            (point.1.1.vocab_richness, point.1.1.compression_ratio),
                            15,
                            ShapeStyle::filled(&original_style),
                        )
                    }),
            )?;
        }

        let img = ImageBuffer::<Rgb<u8>, Vec<u8>>::from_raw(width, height, buffer)
            .ok_or(Err(anyhow!("error obtaining image buffer")));
        let img: ImageBuffer<Rgb<u8>, Vec<u8>> = match img {
            std::result::Result::Ok(res) => res,
            Err(e) => e?,
        };

        let mut png_bytes = Vec::new();
        img.write_to(&mut Cursor::new(&mut png_bytes), image::ImageFormat::Png)?;

        Ok(png_bytes)
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
        println!("get trimmed email bodies");
        self.get_trimmed_email_bodies(email_dataset_path)?;
        println!("calculate features");
        self.calculate_dataset_features()?;
        println!("features calculated");
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
            let dataframe = lazy_frame.select([col("body")]).limit(270).collect()?;

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

pub fn calculate_features(email: &String) -> AnyhowResult<(Vec<u8>, Features)> {
    // -- calculate compression ratio --

    let input = email.as_bytes().to_vec();
    let (compression_length, compressed_email) = compress(&input)?;
    let compression_ratio: f64 = compression_length / input.len() as f64;

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
        .map(|x| x.split_ascii_whitespace().count() as f64)
        .collect::<Vec<f64>>();

    let sentence_count = sentences.len() as f64;
    let mean = word_count / sentence_count;

    let squared_sum = sentence_word_counts
        .iter()
        .fold(0.0, |accum, val| accum + (val - mean).powf(2.0));

    let sentence_variance = squared_sum / (sentence_count - 1.0);

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

fn compress(email_bytes: &Vec<u8>) -> AnyhowResult<(f64, Vec<u8>)> {
    let mut compressor = Compress::new(Compression::best(), false);

    let mut output = vec![0u8; email_bytes.len() + 1024];
    let mut input_offset = 0;
    let mut output_offset = 0;

    loop {
        let input_slice = &email_bytes[input_offset..];
        let mut output_slice = &mut output[output_offset..];

        let prev_total_in = compressor.total_in();
        let prev_total_out = compressor.total_out();

        let status = compressor.compress(
            &input_slice,
            &mut output_slice,
            flate2::FlushCompress::Finish,
        )?;

        input_offset += (compressor.total_in() - prev_total_in) as usize;
        output_offset += (compressor.total_out() - prev_total_out) as usize;

        match status {
            Status::StreamEnd => {
                break;
            }
            Status::Ok => {
                if input_offset >= email_bytes.len() {
                    println!("features status ok. input offest greater than length");
                    break;
                }
                return Err(anyhow::anyhow!("feature Ok Status,perhaps buffer is full?"));
            }
            Status::BufError => output.resize(output.len() * 2 + 1024, 0),
        }
    }
    Ok((output_offset as f64, output))
}
