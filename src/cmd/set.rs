use std::time::Duration;

use tracing::debug;

use crate::{
    connection::Connection,
    error::CacheError,
    parse::Parse,
    storage::{Db, entity::Entity},
};

#[derive(Debug)]
pub(crate) struct Set {
    key: Entity,
    value: Entity,
    expire: Option<Duration>,
}

impl Set {
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Set, CacheError> {
        let key = parse.next()?;
        let value = parse.next()?;

        let mut expire = None;

        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs as u64));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms as u64));
            }
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            Err(CacheError::EndOfStream) => {}
            Err(err) => return Err(err.into()),
        }

        Ok(Set { key, value, expire })
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> Result<(), CacheError> {
        db.set(self.key, self.value, self.expire).await;

        let response = Entity::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }
}
