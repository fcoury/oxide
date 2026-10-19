//! Logical sessions retain a PostgreSQL connection only during a wire transaction.
use crate::handler::{CommandExecutionError, Request};
use crate::pg::PgDb;
use bson::{doc, Document};
use postgres::NoTls;
use r2d2_postgres::PostgresConnectionManager;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub type Connection = r2d2::PooledConnection<PostgresConnectionManager<NoTls>>;
pub type Slot = Arc<Mutex<Option<Connection>>>;
#[derive(Default)]
pub struct Sessions {
    entries: HashMap<Vec<u8>, Arc<Mutex<Session>>>,
}
pub struct Session {
    slot: Slot,
    number: Option<i64>,
    state: State,
    touched: Instant,
}
#[derive(PartialEq)]
enum State {
    Idle,
    Active,
    Committed,
    Aborted,
}
impl Default for Session {
    fn default() -> Self {
        Self {
            slot: Default::default(),
            number: None,
            state: State::Idle,
            touched: Instant::now(),
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.rollback();
    }
}
impl Session {
    fn rollback(&mut self) {
        if let Ok(mut slot) = self.slot.lock() {
            if let Some(mut connection) = slot.take() {
                let _ = connection.batch_execute("ROLLBACK");
            }
        }
        self.state = State::Aborted;
    }
}
impl Sessions {
    pub fn expire(&mut self) {
        self.entries.retain(|_, entry| {
            if let Ok(session) = entry.try_lock() {
                session.touched.elapsed()
                    < Duration::from_secs(if session.state == State::Active {
                        60
                    } else {
                        1800
                    })
            } else {
                true
            }
        });
    }
    pub fn get(&mut self, id: &Document) -> Result<Arc<Mutex<Session>>, CommandExecutionError> {
        if id.len() != 1
            || !matches!(id.get("id"),Some(bson::Bson::Binary(b)) if b.subtype==bson::spec::BinarySubtype::Uuid && b.bytes.len()==16)
        {
            return Err(CommandExecutionError::new(
                "invalid logical session id".into(),
            ));
        }
        self.expire();
        let key = bson::to_vec(id).map_err(|e| CommandExecutionError::new(e.to_string()))?;
        if !self.entries.contains_key(&key) && self.entries.len() >= 512 {
            return Err(CommandExecutionError::new(
                "logical session budget exceeded".into(),
            ));
        }
        Ok(self.entries.entry(key).or_default().clone())
    }
    pub fn end(&mut self, id: &Document) {
        if let Ok(key) = bson::to_vec(id) {
            self.entries.remove(&key);
        }
    }
}
fn no_transaction() -> CommandExecutionError {
    CommandExecutionError {
        message: "transaction is not active".into(),
        code: 251,
        code_name: "NoSuchTransaction".into(),
    }
}

pub fn run(
    request: &Request,
    docs: &[Document],
    execute: impl FnOnce(&Request, &[Document]) -> Result<Document, CommandExecutionError>,
) -> Result<Document, CommandExecutionError> {
    let original = &docs[0];
    let command = original.keys().next().map(String::as_str).unwrap_or("");
    if command == "endSessions" {
        let mut cursors = request
            .cursors
            .lock()
            .map_err(|_| CommandExecutionError::new("session lock unavailable".into()))?;
        for id in original.get_array("endSessions")? {
            if let Some(id) = id.as_document() {
                cursors.sessions.end(id);
            }
        }
        return Ok(doc! {"ok":1.0});
    }
    let Some(id) = original.get("lsid") else {
        return execute(request, docs);
    };
    let id = id
        .as_document()
        .ok_or_else(|| CommandExecutionError::new("lsid must be a document".into()))?;
    let entry = request
        .cursors
        .lock()
        .map_err(|_| CommandExecutionError::new("session lock unavailable".into()))?
        .sessions
        .get(id)?;
    let mut session = entry
        .lock()
        .map_err(|_| CommandExecutionError::new("session unavailable".into()))?;
    session.touched = Instant::now();
    let mut clean = original.clone();
    clean.remove("lsid");
    if !original.contains_key("autocommit") {
        // Retryable writes need a replay ledger and are deliberately not advertised.
        if original.contains_key("txnNumber") {
            return Err(CommandExecutionError::new(
                "retryable writes are not supported; set retryWrites=false".into(),
            ));
        }
        return execute(request, &[clean]);
    }
    if original.get_bool("autocommit")? {
        return Err(CommandExecutionError::new(
            "autocommit must be false".into(),
        ));
    }
    let number = original.get_i64("txnNumber")?;
    clean.remove("autocommit");
    clean.remove("txnNumber");
    let start = match clean.remove("startTransaction") {
        None => false,
        Some(bson::Bson::Boolean(true)) => true,
        _ => {
            return Err(CommandExecutionError::new(
                "startTransaction must be true".into(),
            ))
        }
    };
    let mut owner = bson::to_vec(id).map_err(|e| CommandExecutionError::new(e.to_string()))?;
    owner.extend_from_slice(&number.to_le_bytes());
    if start {
        if session.number.is_some_and(|old| number <= old) {
            return Err(no_transaction());
        }
        if let Some(concern) = clean.remove("readConcern") {
            let concern = concern
                .as_document()
                .ok_or_else(|| CommandExecutionError::new("invalid readConcern".into()))?;
            if concern.len() != 1
                || !matches!(
                    concern.get_str("level"),
                    Ok("local" | "majority" | "snapshot")
                )
            {
                session.rollback();
                return Err(CommandExecutionError::new(
                    "unsupported transaction readConcern".into(),
                ));
            }
        }
        session.rollback();
        let mut client = request.get_client()?;
        client.begin_wire_transaction()?;
        *session
            .slot
            .lock()
            .map_err(|_| CommandExecutionError::new("session unavailable".into()))? =
            Some(client.into_connection());
        session.number = Some(number);
        session.state = State::Active;
    }
    if session.number != Some(number) {
        return Err(no_transaction());
    }
    if command == "commitTransaction" {
        if let Some(concern) = clean.remove("writeConcern") {
            let concern = concern
                .as_document()
                .ok_or_else(|| CommandExecutionError::new("invalid commit writeConcern".into()))?;
            for (key, value) in concern {
                let valid = match key.as_str() {
                    "w" => {
                        matches!(value, bson::Bson::Int32(1) | bson::Bson::Int64(1))
                            || value.as_str() == Some("majority")
                    }
                    "j" => value.as_bool().is_some(),
                    "wtimeout" => crate::pg::integer(value).is_ok_and(|v| v >= 0),
                    _ => false,
                };
                if !valid {
                    return Err(CommandExecutionError::new(
                        "unsupported commit writeConcern".into(),
                    ));
                }
            }
        }
        crate::commands::check_options(&clean, &["commitTransaction"])?;
        if session.state == State::Committed {
            return Ok(doc! {"ok":1.0});
        }
        if session.state != State::Active {
            return Err(no_transaction());
        }
        let mut client = PgDb::from_session(session.slot.clone())?;
        let result = client.commit_wire_transaction();
        drop(client);
        if let Err(error) = result {
            session.rollback();
            return Err(error.into());
        }
        session
            .slot
            .lock()
            .map_err(|_| CommandExecutionError::new("session unavailable".into()))?
            .take();
        session.state = State::Committed;
        request
            .cursors
            .lock()
            .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
            .end_transaction(&owner);
        return Ok(doc! {"ok":1.0});
    }
    if command == "abortTransaction" {
        crate::commands::check_options(&clean, &["abortTransaction"])?;
        if session.state != State::Active {
            return Err(no_transaction());
        }
        session.rollback();
        request
            .cursors
            .lock()
            .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
            .end_transaction(&owner);
        return Ok(doc! {"ok":1.0});
    }
    if session.state != State::Active {
        return Err(no_transaction());
    }
    if ![
        "find",
        "aggregate",
        "getMore",
        "killCursors",
        "insert",
        "update",
        "delete",
        "findAndModify",
        "findandmodify",
        "count",
    ]
    .contains(&command)
    {
        session.rollback();
        return Err(CommandExecutionError::new(
            "command is not supported in a transaction".into(),
        ));
    }
    let request = request.with_session(session.slot.clone(), owner.clone());
    let result = execute(&request, &[clean]).and_then(|reply| {
        if let Ok(errors) = reply.get_array("writeErrors") {
            if errors.iter().any(|error| {
                error
                    .as_document()
                    .is_some_and(|error| error.get_i32("code") == Ok(112))
            }) {
                return Err(CommandExecutionError {
                    message: "transaction write conflict".into(),
                    code: 112,
                    code_name: "WriteConflict".into(),
                });
            }
        }
        Ok(reply)
    });
    if let Ok(reply) = &result {
        if reply.contains_key("cursor") {
            request
                .cursors
                .lock()
                .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
                .own(reply, Some(&owner))?;
        }
    }

    if result
        .as_ref()
        .map_or(true, |reply| reply.contains_key("writeErrors"))
    {
        session.rollback();
        request
            .cursors
            .lock()
            .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
            .end_transaction(&owner);
    }
    result
}
