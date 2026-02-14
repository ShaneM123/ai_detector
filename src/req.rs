use std::collections::HashMap;

use anyhow::{Ok, Result as AnyhowResult};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Request {
    pub method: Method,
    pub path: String,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum Method {
    Get,
}

impl TryFrom<&str> for Method {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "GET" => Ok(Method::Get),
            m => Err(anyhow::anyhow!("unsupported methods: {m}")),
        }
    }
}

pub async fn parse_request(mut stream: impl AsyncBufRead + Unpin) -> AnyhowResult<Request> {
    let mut line_buffer = String::new();

    stream.read_line(&mut line_buffer).await?;

    let mut parts = line_buffer.split_whitespace();

    let method: Method = parts
        .next()
        .ok_or(anyhow::anyhow!("missing method"))
        .and_then(TryInto::try_into)?;

    let path: String = parts
        .next()
        .ok_or(anyhow::anyhow!("missing path"))
        .map(Into::into)?;

    let mut headers = HashMap::new();

    //TODO: move this over to the handler i guess
    loop {
        line_buffer.clear();
        stream.read_line(&mut line_buffer).await?;

        if line_buffer.is_empty() || line_buffer == "\n" || line_buffer == "\r\n" {
            break;
        }
        let mut comps = line_buffer.split(":");

        let key = comps.next().ok_or(anyhow::anyhow!("missing headers"))?;

        let value = comps
            .next()
            .ok_or(anyhow::anyhow!("missing header value"))?
            .trim();

        headers.insert(key.to_string(), value.to_string());
    }

    Ok(Request {
        method,
        path,
        headers,
    })
}
