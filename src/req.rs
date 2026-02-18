use crate::homepage::homepage;
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
        while let Some(email) = request.body_mut().data().await {
            email_gathered.push(email?);
        }
    } else {
        return Ok("422 unprocessable".to_string());
    }

    return Err(anyhow!("something went really wrong matching the method"));
}
