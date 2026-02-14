use std::{sync::Arc, time::Duration};

use ai_detector::{EmailDataset, EmailDropGuard};
use anyhow::{Ok, Result as AnyhowResult};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, broadcast, mpsc},
    time,
};
use tracing::{error, info};

use crate::{connections::Connection, req, shutdown::Shutdown};

struct Listener {
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

            let (socket, addr) = self.accept().await?;
            let mut handler: Handler = Handler {
                email_dataset: self.email_dataset_holder.email_dataset(),
                connection: Connection::new_connection(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
                drop(permit);
            });
        }
    }

    async fn accept(&mut self) -> AnyhowResult<(TcpStream, std::net::SocketAddr)> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                std::result::Result::Ok((socket, addr)) => return Ok((socket, addr)),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;

            backoff *= 2;
        }
    }
}
struct Handler {
    email_dataset: EmailDataset,
    //todo: create the rs files for this
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    async fn run(&mut self) -> AnyhowResult<()> {
        while !self.shutdown.is_shutdown() {
            let maybe_request = tokio::select! {
                res = self.connection.read_req() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            let request = match maybe_request {
                Some(request) => request,
                None => return Ok(()),
            };
        }
        Ok(())
    }
}

const MAX_CONNECTIONS: usize = 250;

pub async fn run(listener: TcpListener, shutdown: impl Future) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx_) = mpsc::channel(1);

    let mut server = Listener {
        listener,
        //todo: pass email dataset
        email_dataset_holder: EmailDropGuard::new(),
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
}
