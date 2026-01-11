use crate::{
    connection::Connection,
    error::CacheError,
    parse::Parse,
    storage::{Db, entity::Entity},
};

#[derive(Debug)]
pub(crate) struct Publish {
    channel: String,
    message: Entity,
}

impl Publish {
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Publish, CacheError> {
        let channel = parse.next_string()?;
        let message = parse.next()?;
        Ok(Publish { channel, message })
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> Result<(), CacheError> {
        let num_subscribers = db.publish(&self.channel, self.message).await;

        let response = Entity::Integer(num_subscribers as i64);
        dst.write_frame(&response).await?;

        Ok(())
    }
}
