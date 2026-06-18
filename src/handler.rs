use std::num::NonZero;
use std::time::Duration;

use crate::homepage::homepage;
use crate::shutdown::{self, Shutdown};
use ai_detector::{EmailDataset, EmailDropGuard, calculate_features};
use anyhow::{Ok, Result as AnyhowResult, anyhow};
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use form_urlencoded;
use governor::{Jitter, Quota};
use h2::RecvStream;
use h2::server::{self, Connection};
use http::{HeaderMap, Method, Request};
use http::{Response, StatusCode};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_rustls::server::TlsStream;
use tracing::info;

//TODO: email input validation via Type Email(String) and deserialse/parse with validation built in basically
pub struct Handler {
    email_dataset: EmailDropGuard,
    //todo: create the rs files for this
    connection: server::Connection<TlsStream<TcpStream>, Bytes>,
    shutdown: Shutdown,
    shutdown_complete: mpsc::Sender<()>,
    origin: String,
    headers: HeaderMap,
}

impl Handler {
    pub fn new(
        email_dataset: EmailDropGuard,
        connection: Connection<TlsStream<TcpStream>, Bytes>,
        shutdown: shutdown::Shutdown,
        shutdown_complete: mpsc::Sender<()>,
        origin: String,
        headers: HeaderMap,
    ) -> Handler {
        Handler {
            email_dataset,
            connection,
            shutdown,
            shutdown_complete,
            origin,
            headers,
        }
    }

    pub async fn run(&mut self) -> AnyhowResult<()> {
        info!("apply ratelimiting");
        let limiter = governor::RateLimiter::direct(
            Quota::per_second(NonZero::new(15).unwrap()).allow_burst(NonZero::new(10).unwrap()),
        );
        info!("run handler");
        while !self.shutdown.is_shutdown() {
            limiter
                .until_ready_with_jitter(Jitter::up_to(Duration::from_millis(3_569)))
                .await;
            let maybe_request = tokio::select! {

                res =  self.connection.accept() => res,
                _ = self.shutdown.recv() => {
                    info!("shutting down handler");
                    self.shutdown_complete.send(()).await?;
                    return Ok(());
                }
            };

            let (request, mut respond) = match maybe_request {
                Some(request) => request?,
                None => return Ok(()),
            };

            let response_builder = Response::builder()
                .header("Access-Control-Allow-Origin", &self.origin)
                .header(
                    "Strict-Transport-Security",
                    "max-age=63072000; includeSubDomains; preload",
                )
                .header("X-Content-Type-Options", "nosniff")
                .header("X-Frame-Options", "SAMEORIGIN")
                .header("Referrer-Policy", "strict-origin-when-cross-origin")
                //TODO: be better to mutate this individually
                .header(
                    "Content-Security-Policy",
                    "default-src 'self'; img-src 'self' data:; form-action 'self'; style-src-attr 'unsafe-hashes' 'sha256-8Y8ZIhn++zkT/pWX+ksEvIwQkmkScdZ1N7zDI23txek=';",
                )
                .header("Content-Language", "en");

            match Self::check_request_headers(&request, &self.origin) {
                std::result::Result::Ok(()) => {}
                Err(e) => {
                    let response = response_builder.status(412).body(())?;

                    respond.send_response(response, true)?;
                    return Err(anyhow!("ERR reading headers: {}", e));
                }
            }

            let html_response = process_request(request).await?;

            let _ = match html_response.body.expect("empty response body") {
                ResponseBodyType::Email(email) => {
                    let mut input_dataset = EmailDataset::new();
                    let input_features = calculate_features(&email)?;
                    input_dataset
                        .features_map
                        .insert(input_features.0, (email.clone(), input_features.1));
                    input_dataset.email_bodies.push(email);
                    info!("ANALYSING EMAIL");

                    let email_clone = self.email_dataset.emails.clone();

                    let res = tokio::task::spawn_blocking(move || {
                        let guard = email_clone;
                        guard.analyse(input_dataset)
                    })
                    .await??;
                    let hompage_html = homepage()?;
                    let mut body = Bytes::new();
                    if res.0 {
                        body = Bytes::from(format!("{} <p>It's a real email</p>", hompage_html));
                    } else {
                        body = Bytes::from(format!("{} <p>It's an AI email</p>", hompage_html));
                    }

                    let response = response_builder
                        .header("Content-Type", "text/html")
                        .status(html_response.status)
                        .body(())?;
                    let mut send_stream = respond.send_response(response, false)?;
                    send_stream.send_data(body, false)?;
                    let encoded = general_purpose::STANDARD.encode(res.1);

                    send_stream.send_data(
                        Bytes::from(format!(
                            "<img src=\"data:image/png;base64,{}\" style=\"max-width: 80%; height: 80%; display: block;\" alt=\"Embedded Image\">",
                            encoded
                        )),
                        true,
                    )?;
                }

                ResponseBodyType::Html(html) => {
                    let response = response_builder
                        .header("Content-Type", "text/html")
                        .status(html_response.status)
                        .body(())?;
                    let mut send_stream = respond.send_response(response, false)?;
                    send_stream.send_data(Bytes::from(html), true)?;
                }

                ResponseBodyType::Image(image) => {
                    let response = response_builder
                        .header("Content-Type", "image/png")
                        .status(html_response.status)
                        .body(())?;
                    let mut send_stream = respond.send_response(response, false)?;

                    send_stream.send_data(Bytes::from(image), true)?;
                }
            };
        }
        Ok(())
    }

    fn check_request_headers(request: &Request<RecvStream>, origin: &String) -> AnyhowResult<()> {
        if request
            .uri()
            .authority()
            .map(|x| x.host().to_ascii_lowercase() != origin.to_ascii_lowercase())
            .is_none_or(|x| !x)
        {
            return Err(anyhow!("authority read fail"));
        }

        //TODO: fix Header, Origin isnt always required or sent and can be spoofed anyway
        if request
            .headers()
            .get("Origin")
            .map(|x| x.as_bytes() == origin.as_bytes())
            .is_some_and(|x| !x)
        {
            info!(
                "{}",
                request.headers().get("Origin").unwrap().to_str().unwrap()
            );
            return Err(anyhow!("Origin header read fail"));
        }

        return Ok(());
    }
}

#[derive(Debug)]
pub struct ResponseHandle {
    status: StatusCode,
    body: Option<ResponseBodyType>,
}

#[derive(Debug)]
enum ResponseBodyType {
    Email(Vec<u8>),
    Html(String),
    Image(Vec<u8>),
}

pub async fn process_request(mut request: Request<RecvStream>) -> AnyhowResult<ResponseHandle> {
    if Method::GET == *request.method() {
        if request.uri().path() == "/" {
            info!("homepage request");
            let hompage_html = homepage()?;

            return Ok(ResponseHandle {
                status: StatusCode::OK,
                body: Some(ResponseBodyType::Html(hompage_html)),
            });
        } else if request.uri().path().contains("favicon.ico") {
            let favicon = tokio::fs::read("cuddlyferris.png").await?;
            return Ok(ResponseHandle {
                status: StatusCode::OK,
                body: Some(ResponseBodyType::Image(favicon)),
            });
        } else {
            let req_path = request.uri().path();
            info!("path was: {}", req_path);
            return Ok(ResponseHandle {
                status: StatusCode::OK,
                body: Some(ResponseBodyType::Html("<Html></Html>".to_string())),
            });
        }
    } else if Method::POST == *request.method() {
        if request.uri().path() == "/submit" {
            let mut email_gathered = Vec::new();
            while let Some(chunk) = request.body_mut().data().await {
                let chunk = chunk?;

                email_gathered.extend_from_slice(&chunk);
                let _ = request
                    .body_mut()
                    .flow_control()
                    .release_capacity(chunk.len())?;
                if email_gathered.len() > 4000 {
                    return Ok(ResponseHandle {
                        status: StatusCode::OK,
                        body: Some(ResponseBodyType::Html(
                            "<Html><div><p>email too long, try a shorter one</p></div></Html>"
                                .to_string(),
                        )),
                    });
                }
            }
            if email_gathered.len() < 16 {
                return Ok(ResponseHandle {
                    status: StatusCode::OK,
                    body: Some(ResponseBodyType::Html(
                        "<Html><div><p>email too short, try a longer one</p></div></Html>"
                            .to_string(),
                    )),
                });
            }

            let decoded_email = match form_urlencoded::parse(&email_gathered).next() {
                Some(email) => email.1.as_bytes().to_vec(),
                None => {
                    return Ok(ResponseHandle {
                        status: StatusCode::NO_CONTENT,
                        body: Some(ResponseBodyType::Html(
                            "<Html><div><p>status 204</p></div></Html>".to_string(),
                        )),
                    });
                }
            };

            return Ok(ResponseHandle {
                body: Some(ResponseBodyType::Email(decoded_email)),
                status: StatusCode::OK,
            });
        }
    } else {
        //TODO: return 422
        return Ok(ResponseHandle {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body: Some(ResponseBodyType::Html(
                "<Html><div><p>status 422</p></div></Html>".to_string(),
            )),
        });
    }
    info!("returning empty response");
    return Ok(ResponseHandle {
        status: StatusCode::NOT_FOUND,
        body: None,
    });
}
