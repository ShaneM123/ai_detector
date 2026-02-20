use crate::homepage::homepage;
use ai_detector::Emails;
use anyhow::anyhow;
use anyhow::{Ok, Result as AnyhowResult};
use bytes::Bytes;
use h2::RecvStream;
use h2::server::{self, Connection};
use http::{Method, Request};
use http::{Response, StatusCode};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_rustls::server::TlsStream;

use tracing::info;

use crate::shutdown::{self, Shutdown};

pub struct Handler {
    email_dataset: Emails,
    //todo: create the rs files for this
    connection: server::Connection<TlsStream<TcpStream>, Bytes>,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    pub fn new(
        email_dataset: Emails,
        connection: Connection<TlsStream<TcpStream>, Bytes>,
        shutdown: shutdown::Shutdown,
        _shutdown_complete: mpsc::Sender<()>,
    ) -> Handler {
        Handler {
            email_dataset: email_dataset,
            connection,
            shutdown,
            _shutdown_complete,
        }
    }

    pub async fn run(&mut self) -> AnyhowResult<()> {
        info!("run handler");
        while !self.shutdown.is_shutdown() {
            let maybe_request = tokio::select! {

                res =  self.connection.accept() => res,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            let (request, mut respond) = match maybe_request {
                Some(request) => request?,
                None => return Ok(()),
            };

            let html_response = process_request(request).await?;

            let response_body = match html_response.body.expect("empty response body") {
                ResponseBodyType::Email(email) => {
                    self.email_dataset.set_input(email)?;
                    //TODO: spawn a cpu std::thread for sync cpu task
                    if self.email_dataset.analyse().await? {
                        "It's a real email".to_string()
                    } else {
                        "It's an AI email".to_string()
                    }
                }
                ResponseBodyType::Html(html) => html,
            };

            let response: Response<()> = Response::builder().status(StatusCode::OK).body(())?;

            let mut resp_res = respond.send_response(response, false)?;
            //response.body(html_response);
            let _ = resp_res.send_data(Bytes::from(response_body), html_response.end_of_stream)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ResponseHandle {
    pub body: Option<ResponseBodyType>,
    end_of_stream: bool,
}

#[derive(Debug)]
enum ResponseBodyType {
    Email(String),
    Html(String),
}

pub async fn process_request(mut request: Request<RecvStream>) -> AnyhowResult<ResponseHandle> {
    if Method::GET == *request.method() {
        if request.uri().path() == "/" {
            let hompage_html = homepage()?;

            return Ok(ResponseHandle {
                body: Some(ResponseBodyType::Html(hompage_html)),
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
            }

            let mut unsanatized_request = String::from_utf8(email_gathered)?;
            info!("POST REQUEST: {}", unsanatized_request);
            let email = unsanatized_request.split_off(5);
            return Ok(ResponseHandle {
                body: Some(ResponseBodyType::Email(email)),
                end_of_stream: true,
            });
        }
    } else {
        //TODO: return 422
        return Err(anyhow!(
            "something went really wrong matching the method {:?}",
            request
        ));
    }
    info!("returning empty response");
    return Ok(ResponseHandle {
        body: None,
        end_of_stream: false,
    });
}
