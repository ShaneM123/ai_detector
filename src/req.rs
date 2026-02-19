use crate::{homepage::homepage, req};
use anyhow::{Ok, Result as AnyhowResult, anyhow};
use h2::RecvStream;
use http::{Method, Request, Response};
use tracing::info;

pub async fn process_request(mut request: Request<RecvStream>) -> AnyhowResult<String> {
    if Method::GET == *request.method() {
        if request.uri().path() == "/" {
            //TODO: might need to tell it not to end the stream here
            let hompage_html = homepage()?;
            return Ok(hompage_html);
        }
    } else if Method::POST == *request.method() {
        let mut email_gathered = Vec::new();
        while let Some(chunk) = request.body_mut().data().await {
            let chunk = chunk?;

            email_gathered.extend_from_slice(&chunk);
            let _ = request
                .body_mut()
                .flow_control()
                .release_capacity(chunk.len())?;
        }
        let unsanatized_request = String::from_utf8(email_gathered)?;
        info!("POST REQUEST: {}", unsanatized_request);
    } else {
        return Ok("422 unprocessable".to_string());
    }

    return Err(anyhow!("something went really wrong matching the method"));
}
