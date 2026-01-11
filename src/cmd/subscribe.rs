use std::pin::Pin;

use bytes::Bytes;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

use crate::{
    cmd::unknown::Unknown,
    connection::Connection,
    error::CacheError,
    parse::{Command, Parse},
    shutdown::Shutdown,
    storage::{Db, entity::Entity},
};

#[derive(Clone, Debug)]
pub(crate) struct Subscribe {
    channels: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct Unsubscribe {
    channels: Vec<String>,
}

type Messages = Pin<Box<dyn Stream<Item = Entity> + Send>>;

impl Subscribe {
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Subscribe, CacheError> {
        let mut channels = vec![parse.next_string()?];
        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(CacheError::EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }
        Ok(Subscribe { channels })
    }

    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> Result<(), CacheError> {
        let mut subscriptions = StreamMap::new();

        loop {
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            tokio::select! {
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        None => return Ok(())
                    };
                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst,
                    ).await?;
                }
                _ = shutdown.recv() => {
                    return Ok(())
                }
            }
        }
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> Result<(), CacheError> {
    let mut rx = db.subscribe(channel_name.clone()).await;

    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });
    subscriptions.insert(channel_name.clone(), rx);

    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

async fn handle_command(
    frame: Entity,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> Result<(), CacheError> {
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }
            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Entity {
    let mut response = Entity::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as i64);
    response
}

fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Entity {
    let mut response = Entity::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as i64);
    response
}
fn make_message_frame(channel_name: String, frame: Entity) -> Entity {
    let mut response = Entity::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push(frame);
    response
}

impl Unsubscribe {
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, CacheError> {
        let mut channels = vec![];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(CacheError::EndOfStream) => break,
                Err(err) => return Err(err),
            }
        }
        Ok(Unsubscribe { channels })
    }
}
