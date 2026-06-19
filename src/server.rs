use std::net::IpAddr;
use std::num::NonZeroU32;
use std::{sync::Arc, time::Duration};

use ai_detector::EmailDropGuard;
use anyhow::{Ok, Result as AnyhowResult, anyhow};
use governor::clock::QuantaClock;
use governor::state::keyed::DashMapStateStore;
use governor::{Jitter, Quota, RateLimiter};
use h2::server::Builder;
use http::{HeaderMap, HeaderName};
use tokio::time::timeout;
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

struct Listener {
    acceptor: TlsAcceptor,
    listener: TcpListener,
    email_dataset_holder: EmailDropGuard,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    origin: String,
    headers: HeaderMap,
    ip_limiter: Arc<RateLimiter<IpAddr, DashMapStateStore<IpAddr>, QuantaClock>>,
}

impl Listener {
    pub async fn run(&mut self) -> AnyhowResult<()> {
        info!("accepting inbound connections");

        while !self.shutdown_complete_tx.is_closed() {
            {
                let permit: tokio::sync::OwnedSemaphorePermit =
                    match self.limit_connections.clone().acquire_owned().await {
                        std::result::Result::Ok(val) => val,

                        Err(e) => {
                            // probably should not continue if the semaphore closes
                            return Err(anyhow!("tried to obtain semaphore, got error: {}", e));
                        }
                    };

                info!("obtaining socket");

                let (socket, addr) = match self.accept().await {
                    std::result::Result::Ok(val) => val,
                    Err(e) => {
                        //TODO: handle each type of error individually where need be
                        error!("tried to accept Stream, got error: {}", e);
                        continue;
                    }
                };

                info!("obtained socket for address {}", addr);
                let stream = match Builder::new()
                    .max_concurrent_streams(150)
                    .initial_connection_window_size(1_000_000)
                    .handshake(socket)
                    .await
                {
                    std::result::Result::Ok(val) => val,
                    Err(e) => {
                        error!(
                            "tried to TLS Handshake Stream on address {} , got error: {}",
                            addr, e
                        );

                        continue;
                    }
                };

                let mut handler = Handler::new(
                    self.email_dataset_holder.clone(),
                    stream,
                    Shutdown::new(self.notify_shutdown.subscribe()),
                    self.shutdown_complete_tx.clone(),
                    self.origin.clone(),
                    self.headers.clone(),
                );

                info!("spawning handler run for address");

                tokio::spawn(timeout(Duration::from_mins(10), async move {
                    if let Err(err) = handler.run().await {
                        error!(cause = ?err, "connection error");
                    }
                    drop(permit);
                }));
            }
        }

        info!(" reciever closed ");
        Ok(())
    }

    async fn accept(&mut self) -> AnyhowResult<(TlsStream<TcpStream>, std::net::SocketAddr)> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                std::result::Result::Ok((socket, addr)) => {
                    match self
                        .ip_limiter
                        .until_key_n_ready_with_jitter(
                            &addr.ip(),
                            NonZeroU32::new(20).unwrap(),
                            Jitter::up_to(Duration::from_secs(15)),
                        )
                        .await
                    {
                        std::result::Result::Ok(k) => k,
                        Err(e) => {
                            return Err(anyhow!("insuffecient limiter bucket capacity: {}", e));
                        }
                    }

                    info!("accepting acceptor");
                    let accepted_stream = match self.acceptor.accept(socket).await {
                        std::result::Result::Ok(val) => val,
                        Err(e) => {
                            return Err(anyhow!(
                                "tls acceptance for address {} , error: {}",
                                addr,
                                e
                            ));
                        }
                    };
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

const MAX_CONNECTIONS: usize = 600;

pub async fn run(server_config: Config, shutdown: impl Future) -> AnyhowResult<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx_) = mpsc::channel(1);

    println!("getting certs");
    let certs =
        CertificateDer::pem_file_iter(server_config.server_cert)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(server_config.server_key)?;

    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    println!("setting headers");

    let mut headers = HeaderMap::new();

    headers.insert(
        HeaderName::from_static("host"),
        server_config.origin.parse().unwrap(),
    );
    headers.insert(
        HeaderName::from_static("accept"),
        "application/json, text/html, */*".parse().unwrap(),
    );
    headers.insert(
        HeaderName::from_static("accept-language"),
        "en-US,en;".parse().unwrap(),
    );
    headers.insert(
        HeaderName::from_static("content-type"),
        "application/x-www-form-urlencoded, multipart/form-data, text/plain"
            .parse()
            .unwrap(),
    );

    let listener = TcpListener::bind(server_config.server_address).await?;

    let acceptor: TlsAcceptor = TlsAcceptor::from(Arc::new(config));

    let ip_limiter: Arc<RateLimiter<IpAddr, DashMapStateStore<IpAddr>, QuantaClock>> =
        Arc::new(RateLimiter::dashmap(
            Quota::per_second(NonZeroU32::new(30).unwrap())
                .allow_burst(NonZeroU32::new(20).unwrap()),
        ));

    let mut server: Listener = Listener {
        acceptor,
        listener,
        email_dataset_holder: EmailDropGuard::new(server_config.emails),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        origin: server_config.origin,
        headers: headers,
        ip_limiter: ip_limiter,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
            let _ = server.notify_shutdown.send(())?;
        }
    }

    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    info!("dropping listener");
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx_.recv().await;

    Ok(())
}
