use tracing::{debug, instrument};

use crate::{connection::Connection, error::CacheError, storage::entity::Entity};

#[derive(Debug)]
pub(crate) struct Unknown {
    command_name: String,
}

impl Unknown {
    pub(crate) fn new(key: impl ToString) -> Self {
        Self {
            command_name: key.to_string(),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }

    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> Result<(), CacheError> {
        let response = Entity::Error(format!("ERR unknown command '{}'", self.command_name));

        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }
}
