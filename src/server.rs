use std::{sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, broadcast, mpsc},
    time,
};
use tracing::{debug, error, info, instrument};

use crate::{
    connection::Connection,
    error::CacheError,
    parse::Command,
    shutdown::Shutdown,
    storage::{Db, DbDropGuard},
};

struct Listener {
    db_holder: DbDropGuard,
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

struct Handler {
    db: Db,
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

const MAX_CONNECTIONS: usize = 256;

pub async fn run(listener: TcpListener, shutdown: impl Future) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!(cause = ?err, "failed to accept");
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

    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    async fn run(&mut self) -> Result<(), CacheError> {
        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;

            let mut handler = Handler {
                db: self.db_holder.db(),
                connection: Connection::new(socket),
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

    async fn accept(&mut self) -> Result<TcpStream, CacheError> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
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

impl Handler {
    #[instrument(skip(self))]
    async fn run(&mut self) -> Result<(), CacheError> {
        while !self.shutdown.is_shutdown() {
            let maybe_entity = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(())
                }
            };

            let entity = match maybe_entity {
                Some(entity) => entity,
                None => return Ok(()),
            };

            let cmd = Command::from_frame(entity)?;

            debug!(?cmd);

            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn key_value_get_set_del() {
        let addr = start_server().await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"$-1\r\n", &response);

        stream
            .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"+OK\r\n", &response);

        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .await
            .unwrap();

        let mut response = [0; 11];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"$5\r\nvalue\r\n", &response);

        stream
            .write_all(b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"+OK\r\n", &response);

        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"$-1\r\n", &response);
    }

    #[tokio::test]
    async fn key_value_timeout() {
        let addr = start_server().await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        stream
            .write_all(b"*5\r\n$3\r\nSET\r\n$5\r\nhello\r\n$5\r\nworld\r\n+EX\r\n:1\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];

        stream.read_exact(&mut response).await.unwrap();

        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 11];

        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"$5\r\nworld\r\n", &response);

        tokio::time::sleep(Duration::from_secs(1)).await;

        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];

        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"$-1\r\n", &response);
    }

    #[tokio::test]
    async fn ping() {
        let addr = start_server().await;

        let mut stream = TcpStream::connect(addr).await.unwrap();

        stream.write_all(b"*1\r\n+PING\r\n").await.unwrap();

        let mut response = [0; 7];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(b"+PONG\r\n", &response);
    }

    #[tokio::test]
    async fn pub_sub() {
        let addr = start_server().await;

        let mut publisher = TcpStream::connect(addr).await.unwrap();

        publisher
            .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$5\r\nHello\r\n$5\r\nworld\r\n")
            .await
            .unwrap();

        let mut response = [0; 4];
        publisher.read_exact(&mut response).await.unwrap();
        assert_eq!(b":0\r\n", &response);

        let mut sub1 = TcpStream::connect(addr).await.unwrap();
        sub1.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 34];
        sub1.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$9\r\nsubscribe\r\n$5\r\nhello\r\n:1\r\n"[..],
            &response[..]
        );

        publisher
            .write_all(b"*3\r\n$7\r\nPUBLISH\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
            .await
            .unwrap();

        let mut response = [0; 4];
        publisher.read_exact(&mut response).await.unwrap();
        assert_eq!(b":1\r\n", &response);

        let mut response = [0; 39];
        sub1.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\nworld\r\n"[..],
            &response[..]
        );

        let mut sub2 = TcpStream::connect(addr).await.unwrap();
        sub2.write_all(b"*3\r\n$9\r\nSUBSCRIBE\r\n$5\r\nhello\r\n$3\r\nfoo\r\n")
            .await
            .unwrap();

        let mut response = [0; 34];
        sub2.read_exact(&mut response).await.unwrap();
        assert_eq!(
            &b"*3\r\n$9\r\nsubscribe\r\n$5\r\nhello\r\n:1\r\n"[..],
            &response[..]
        );
    }

    async fn start_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move { run(listener, tokio::signal::ctrl_c()).await });

        addr
    }
}
