use bytes::BytesMut;
use tokio::{io::BufWriter, net::TcpStream};
use anyhow::{Ok, Result as AnyhowResult};

#[derive(Debug)]
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    stream: BufWriter<TcpStream>,

    // The buffer for reading frames.
    buffer: BytesMut,
}

impl Connection {
    pub fn new_connection(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(400 * 1024),
        }
    }
        pub async fn read_frame(&mut self) -> AnyhowResult<Option(Frame)>{

        }
    }
}
