use bytes::Bytes;
use tracing::debug;

use crate::{connection::Connection, error::CacheError, parse::Parse, storage::entity::Entity};

#[derive(Default, Clone, Debug)]
pub(crate) struct Ping {
    msg: Option<Bytes>,
}

impl Ping {
    pub(crate) fn new(msg: Option<Bytes>) -> Self {
        Self { msg }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Ping, CacheError> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(CacheError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> Result<(), CacheError> {
        let response = match self.msg {
            None => Entity::Simple("PONG".to_string()),
            Some(msg) => Entity::Bulk(msg),
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }
}
