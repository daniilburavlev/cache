use tracing::debug;

use crate::{
    connection::Connection,
    error::CacheError,
    parse::Parse,
    storage::{Db, entity::Entity},
};

#[derive(Debug)]
pub(crate) struct Del {
    key: Entity,
}

impl Del {
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Self, CacheError> {
        let key = parse.next()?;
        Ok(Self { key })
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> Result<(), CacheError> {
        db.del(&self.key).await;
        let response = Entity::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }
}
