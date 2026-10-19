use crate::commands::Handler;
use crate::handler::{CommandExecutionError, Request};
use crate::wire::{MAX_DOCUMENT_LEN, MAX_MSG_LEN};
use bson::{doc, Bson, Document};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct IsMaster {}

impl Handler for IsMaster {
    fn new() -> Self {
        IsMaster {}
    }

    fn handle(
        &self,
        _request: &Request,
        _msg: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        Ok(doc! {
          "ismaster": Bson::Boolean(true),
          "maxBsonObjectSize": MAX_DOCUMENT_LEN,
          "maxMessageSizeBytes": MAX_MSG_LEN,
          "maxWriteBatchSize": 10000,
          "localTime": Bson::DateTime(bson::DateTime::from_millis(local_time.try_into().unwrap())),
          "minWireVersion": 0,
          "maxWireVersion": 13,
            "logicalSessionTimeoutMinutes": 30,
          "readOnly": Bson::Boolean(false),
          "ok": Bson::Double(1.into())
        })
    }
}
