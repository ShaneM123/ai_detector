use std::{path::Path, sync::Arc, time::Duration};

use ai_detector::{EmailDataset, EmailDropGuard, Emails};
use anyhow::{Ok, Result as AnyhowResult};
use h2::server::Builder;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, broadcast, mpsc},
    time,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
    server::TlsStream,
};

use tracing::{error, info};

use crate::{Config, handler::Handler, shutdown::Shutdown};

//TODO: create h2 Connection
// call accept on Connection
// main / acceptor loop
//   ↓ spawns per-connection tasks
// Connection Handler (tokio_rustls + h2::server::Connection)
//   ↓ accepts streams → minimal req conversion → calls app service
// Application Service (axum::Router / tower::Service / custom hyper-like service)
//   ↓ middleware stack
// Route Handlers / Business Logic
//   ↓ DB calls, file serving, external requests, response building
// Body streaming back up the chain → h2 → TLS → TCP
struct Listener {
    acceptor: TlsAcceptor,
    listener: TcpListener,
    email_dataset_holder: EmailDropGuard,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    pub async fn run(&mut self) -> AnyhowResult<()> {
        info!("accepting inbound connections");

        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            info!("obtaining socket");

            let (socket, _addr) = self.accept().await?;
            info!("obtained socket");
            let stream = Builder::new()
                .max_concurrent_streams(150)
                .initial_connection_window_size(1_000_000)
                .handshake(socket)
                .await?;

            let mut handler = Handler::new(
                self.email_dataset_holder.clone(),
                stream,
                Shutdown::new(self.notify_shutdown.subscribe()),
                self.shutdown_complete_tx.clone(),
            );

            info!("spawning handler run");

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
                drop(permit);
            });
        }
    }

    async fn accept(&mut self) -> AnyhowResult<(TlsStream<TcpStream>, std::net::SocketAddr)> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                std::result::Result::Ok((socket, addr)) => {
                    info!("accepting acceptor");
                    let accepted_stream = self.acceptor.accept(socket).await?;
                    info!("returning stream");
                    return Ok((accepted_stream, addr));
                }

                Err(err) => {
                    if backoff > 64 {
                        info!("error and backoff graeter than 64");

                        return Err(err.into());
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;

            backoff *= 2;
        }
    }
}

const MAX_CONNECTIONS: usize = 250;

pub async fn run(server_config: Config, shutdown: impl Future) -> AnyhowResult<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx_) = mpsc::channel(1);

    let certs =
        CertificateDer::pem_file_iter("test_server2.crt")?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file("test_server2.key")?;

    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
    config.alpn_protocols = vec![b"h2".to_vec()];

    let listener = TcpListener::bind(server_config.server_address).await?;

    let acceptor = TlsAcceptor::from(Arc::new(config));

    let mut server: Listener = Listener {
        acceptor,
        listener,
        email_dataset_holder: EmailDropGuard::new(server_config.emails),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx_.recv().await;

    Ok(())
}
