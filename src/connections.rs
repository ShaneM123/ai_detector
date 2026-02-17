use anyhow::{Ok, Result as AnyhowResult};
use bytes::BytesMut;
use tokio::{io::BufReader, net::TcpStream};
use tokio_rustls::server::TlsStream;

use crate::req::{Request, parse_request};

#[derive(Debug)]
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    stream: BufReader<TlsStream<TcpStream>>,

    // The buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    pub fn new_connection(socket: TlsStream<TcpStream>) -> Connection {
        Connection {
            stream: BufReader::new(socket),
            buffer: BytesMut::with_capacity(400 * 1024),
        }
    }

    pub async fn read_req(&mut self) -> AnyhowResult<Option<Request>> {
        Ok(Some(parse_request(&mut self.stream).await?))
    }
}
