use crate::handler::CommandExecutionError;
use crate::wire::MAX_DOCUMENT_LEN;
use crate::{commands::Handler, handler::Request};
use bson::{doc, Bson, Document};

const MONGO_DB_VERSION: &str = "5.0.0";

pub struct BuildInfo {}

impl Handler for BuildInfo {
    fn new() -> Self {
        BuildInfo {}
    }

    fn handle(
        &self,
        _request: &Request,
        _msg: &[Document],
    ) -> Result<Document, CommandExecutionError> {
        Ok(doc! {
            "version": MONGO_DB_VERSION,
            "gitVersion": "unknown",
            "oxideVersion": env!("CARGO_PKG_VERSION"),
            "storageVersion": crate::serializer::STORAGE_VERSION,
            "modules": Bson::Array(vec![]),
            "sysInfo": "deprecated",
            "versionArray": Bson::Array(vec![
                Bson::Int32(5),
                Bson::Int32(0),
                Bson::Int32(0),
                Bson::Int32(0),
            ]),
            "bits": Bson::Int32(64),
            "debug": false,
            "maxBsonObjectSize": Bson::Int32(MAX_DOCUMENT_LEN.try_into().unwrap()),
            "buildEnvironment": doc!{},

            // our extensions
            // "ferretdbVersion", version.Get().Version,

            "ok": Bson::Double(1.0)
        })
    }
}
