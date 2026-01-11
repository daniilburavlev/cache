use tracing::{debug, instrument};

use crate::{
    connection::Connection,
    error::CacheError,
    parse::Parse,
    storage::{Db, entity::Entity},
};

#[derive(Debug)]
pub(crate) struct Get {
    key: Entity,
}

impl Get {
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Get, CacheError> {
        let key = parse.next()?;
        Ok(Get { key })
    }

    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> Result<(), CacheError> {
        let response = if let Some(value) = db.get(&self.key).await {
            value
        } else {
            Entity::Null
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }
}
