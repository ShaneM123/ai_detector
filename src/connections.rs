use bytes::BytesMut;
use tokio::{io::BufReader, net::TcpStream};
use tokio_rustls::server::TlsStream;

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
}
