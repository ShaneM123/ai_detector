use std::num::NonZero;
use std::time::Duration;

use crate::homepage::homepage;
use crate::shutdown::{self, Shutdown};
use ai_detector::{EmailDropGuard, Emails};
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
//TODO: some basic bot prevention using governor crate
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
                .until_ready_with_jitter(Jitter::up_to(Duration::from_millis(2_569)))
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
                .header(
                    "Content-Security-Policy",
                    "default-src 'self'; form-action 'self';",
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
                    {
                        let mut guard: tokio::sync::MutexGuard<'_, Emails> =
                            self.email_dataset.emails.lock().await;
                        guard.set_input(email)?;
                    }
                    info!("ANALYSING EMAIL");

                    let email_clone = self.email_dataset.emails.clone();

                    let res = tokio::task::spawn_blocking(move || {
                        let guard = email_clone.blocking_lock();
                        guard.analyse()
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
                            "<img src=\"data:image/png;base64,{}\" style=\"max-width: 80%; height: auto; display: block;\" alt=\"Embedded Image\">",
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
            .map(|x| x.as_str().to_ascii_lowercase() != origin.to_ascii_lowercase())
            .is_none_or(|x| !x)
        {
            return Err(anyhow!("authority read fail"));
        }
        //request.headers().iter().any(|x|   ALLOWED_HEADERS.ix.0.as_str());

        if request
            .headers()
            .get("Origin")
            .map(|x| x.as_bytes() == origin.as_bytes())
            .is_none_or(|x| !x)
        {
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
    Email(String),
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
            if email_gathered.len() < 6 {
                return Ok(ResponseHandle {
                    status: StatusCode::OK,
                    body: Some(ResponseBodyType::Html(
                        "<Html><div><p>email too short, try a longer one</p></div></Html>"
                            .to_string(),
                    )),
                });
            }
            info!("email_gathered {}", email_gathered.len());
            let email = form_urlencoded::parse(&email_gathered)
                .into_iter()
                .map(|x| x.1)
                .collect::<String>();
            info!("EMAIL: {}", email);
            return Ok(ResponseHandle {
                body: Some(ResponseBodyType::Email(email)),
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
