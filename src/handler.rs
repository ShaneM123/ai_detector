use std::sync::Arc;

use crate::homepage::homepage;
use crate::shutdown::{self, Shutdown};
use ai_detector::{EmailDropGuard, Emails};
use anyhow::Result;
use anyhow::anyhow;
use anyhow::{Ok, Result as AnyhowResult};
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use form_urlencoded;
use h2::RecvStream;
use h2::server::{self, Connection};
use http::{Method, Request};
use http::{Response, StatusCode};
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::{net::TcpStream, sync::mpsc};
use tokio_rustls::server::TlsStream;
use tracing::info;

pub struct Handler {
    email_dataset: EmailDropGuard,
    //todo: create the rs files for this
    connection: server::Connection<TlsStream<TcpStream>, Bytes>,
    shutdown: Shutdown,
    shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    pub fn new(
        email_dataset: EmailDropGuard,
        connection: Connection<TlsStream<TcpStream>, Bytes>,
        shutdown: shutdown::Shutdown,
        shutdown_complete: mpsc::Sender<()>,
    ) -> Handler {
        Handler {
            email_dataset,
            connection,
            shutdown,
            shutdown_complete,
        }
    }

    pub async fn run(&mut self) -> AnyhowResult<()> {
        info!("run handler");
        while !self.shutdown.is_shutdown() {
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

            let html_response = process_request(request).await?;

            let response: Response<()> = Response::builder()
                .header("Content-Type", "text/html")
                .status(html_response.status)
                .body(())?;

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

                    let mut send_stream = respond.send_response(response, false)?;
                    send_stream.send_data(body, false)?;
                    let encoded = general_purpose::STANDARD.encode(res.1);

                    send_stream.send_data(
                        Bytes::from(format!(
                            "<img src=\"data:image/png;base64,{}\" alt=\"Embedded Image\">",
                            encoded
                        )),
                        true,
                    )?;
                }

                ResponseBodyType::Html(html) => {
                    let mut send_stream = respond.send_response(response, false)?;
                    send_stream.send_data(Bytes::from(html), true)?;
                }

                ResponseBodyType::Image(image) => {
                    let response: Response<()> = Response::builder()
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
}

#[derive(Debug)]
pub struct ResponseHandle {
    status: StatusCode,
    pub body: Option<ResponseBodyType>,
    end_of_stream: bool,
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
                end_of_stream: true,
            });
        } else if request.uri().path().contains("favicon.ico") {
            let favicon = tokio::fs::read("cuddlyferris.png").await?;
            return Ok(ResponseHandle {
                status: StatusCode::OK,
                body: Some(ResponseBodyType::Image(favicon)),
                end_of_stream: true,
            });
        } else {
            let req_path = request.uri().path();
            info!("path was: {}", req_path);
            return Ok(ResponseHandle {
                status: StatusCode::OK,
                body: Some(ResponseBodyType::Html("<Html></Html>".to_string())),
                end_of_stream: true,
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
                        end_of_stream: true,
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
                    end_of_stream: true,
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
                end_of_stream: true,
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
            end_of_stream: true,
        });
    }
    info!("returning empty response");
    return Ok(ResponseHandle {
        status: StatusCode::NOT_FOUND,
        body: None,
        end_of_stream: true,
    });
}
