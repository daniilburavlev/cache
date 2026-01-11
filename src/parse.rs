use bytes::Bytes;

use crate::{
    cmd::{
        del::Del,
        get::Get,
        ping::Ping,
        publish::Publish,
        set::Set,
        subscribe::{Subscribe, Unsubscribe},
        unknown::Unknown,
    },
    connection::Connection,
    error::CacheError,
    shutdown::Shutdown,
    storage::{Db, entity::Entity},
};
use std::vec;

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Publish(Publish),
    Set(Set),
    Del(Del),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Entity) -> Result<Command, CacheError> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "del" => Command::Del(Del::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            _ => {
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.finish()?;

        Ok(command)
    }

    pub fn get_name(&self) -> &str {
        match self {
            Command::Set(_) => "set",
            Command::Get(_) => "get",
            Command::Del(_) => "del",
            Command::Publish(_) => "pub",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubsribe",
            Command::Ping(_) => "ping",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }

    pub async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<(), CacheError> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Del(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
            Unsubscribe(_) => Err("`Unsubsribe` is unsuppored in this context".into()),
        }
    }
}

pub(crate) struct Parse {
    parts: vec::IntoIter<Entity>,
}

impl Parse {
    pub(crate) fn new(frame: Entity) -> Result<Parse, CacheError> {
        let array = match frame {
            Entity::Array(arr) => arr,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    pub(crate) fn next(&mut self) -> Result<Entity, CacheError> {
        self.parts.next().ok_or(CacheError::EndOfStream)
    }

    pub(crate) fn next_string(&mut self) -> Result<String, CacheError> {
        match self.next()? {
            Entity::Simple(s) => Ok(s),
            Entity::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    pub(crate) fn next_int(&mut self) -> Result<i64, CacheError> {
        match self.next()? {
            Entity::Integer(i) => Ok(i),
            frame => Err(format!("protocol error; expected number, got {:?}", frame).into()),
        }
    }

    pub(crate) fn finish(&mut self) -> Result<(), CacheError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }

    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, CacheError> {
        match self.next()? {
            Entity::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Entity::Bulk(data) => Ok(data),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            )
            .into()),
        }
    }
}
