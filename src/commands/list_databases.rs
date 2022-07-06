use crate::commands::Handler;
use crate::wire::UnknownCommandError;
use bson::{doc, Bson, Document};

pub struct ListDatabases {}

impl Handler for ListDatabases {
  fn new() -> Self {
    ListDatabases {}
  }

  fn handle(&self, _msg: Document) -> Result<Document, UnknownCommandError> {
    let size = 1024 * 1024 * 1024;
    let databases = Bson::Array(vec![
      doc! {
        "name": "mydb1",
        "sizeOnDisk": Bson::Int64(size),
        "empty": false,
      }
      .into(),
      doc! {
        "name": "mydb2",
        "sizeOnDisk": Bson::Int64(size),
        "empty": false,
      }
      .into(),
    ]);
    Ok(doc! {
      "databases": databases,
      "totalSize": Bson::Int64(size),
      "totalSizeMb": Bson::Int64(size/1024/1024),
      "ok": Bson::Double(1.0),
    })
  }
}
