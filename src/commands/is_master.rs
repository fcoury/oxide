use crate::commands::Handler;
use crate::wire::{UnknownCommandError, MAX_DOCUMENT_LEN, MAX_MSG_LEN};
use bson::{doc, Bson, Document};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct IsMaster {}

impl Handler for IsMaster {
  fn new() -> Self {
    IsMaster {}
  }

  fn handle(&self, _msg: Document) -> Result<Document, UnknownCommandError> {
    let local_time = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_millis();
    Ok(doc! {
      "ismaster": Bson::Boolean(true),
      "maxBsonObjectSize": MAX_DOCUMENT_LEN,
      "maxMessageSizeBytes": MAX_MSG_LEN,
      "maxWriteBatchSize": 100000,
      "localTime": Bson::Int64(local_time.try_into().unwrap()),
      "minWireVersion": 0,
      "maxWireVersion": 13,
      "readOnly": Bson::Boolean(false),
      "ok": Bson::Double(1.0)
    })
  }
}
