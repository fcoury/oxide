//! Durable change streams publish PostgreSQL transactional logical messages in
//! commit order. A replication slot retains unacknowledged WAL; journal writes
//! are idempotent by message LSN and commit before acknowledging the slot.
use crate::{
    commands::{check_options, cursor_batch_size},
    deserializer::PostgresJsonDeserializer,
    handler::{CommandExecutionError, Request},
    pg::{PgDb, SqlParam},
    serializer::PostgresSerializer,
};
use bson::{doc, Bson, Document};
use eyre::{bail, Result};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct Stream {
    pub namespace: String,
    pub touched: Instant,
    pub last: i64,
    pub epoch: String,
    pub lookup: bool,
    pub pipeline: Vec<Bson>,
}
const LOCK: i64 = 0x4f58494445;
const VERSION: &str = "oxide:change-journal:v2";
const SLOT: &str = "oxide_changes_v2";
const PUBLICATION: &str = "oxide_changes_v2";
pub fn initialize(client: &mut PgDb) -> Result<()> {
    let level: String = client.raw_query("SHOW wal_level", &[])?[0].get(0);
    if level != "logical" {
        bail!("Oxide change streams require PostgreSQL wal_level=logical and an available replication slot; restart PostgreSQL with that setting");
    }
    // Slot names are cluster-global, so include the PostgreSQL database OID.
    let slot = slot_name(client)?;
    let exists: bool = client.raw_query(
        "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname=$1)",
        &[&PUBLICATION],
    )?[0]
        .get(0);
    if !exists {
        client.batch_execute("CREATE PUBLICATION oxide_changes_v2")?;
    }
    let slots=client.raw_query("SELECT plugin,wal_status FROM pg_replication_slots WHERE slot_name=$1 AND database=current_database()",&[&slot])?;
    if let Some(row) = slots.first() {
        let plugin: String = row.get(0);
        let status: Option<String> = row.get(1);
        if plugin != "pgoutput" || status.as_deref() == Some("lost") {
            bail!("Oxide change-stream replication slot is incompatible or has lost WAL; restore journal continuity before restarting");
        }
    } else {
        client.raw_query(
            "SELECT slot_name FROM pg_create_logical_replication_slot($1,'pgoutput')",
            &[&slot],
        )?;
    }
    client.batch_execute("BEGIN; SELECT pg_advisory_xact_lock(1331186000)")?;
    let result = initialize_inner(client);
    if result.is_ok() {
        client.batch_execute("COMMIT")?;
    } else {
        let _ = client.batch_execute("ROLLBACK");
    }
    result
}
fn slot_name(client: &mut PgDb) -> Result<String> {
    let oid: i64 = client.raw_query(
        "SELECT oid::bigint FROM pg_database WHERE datname=current_database()",
        &[],
    )?[0]
        .get(0);
    Ok(format!("{SLOT}_{oid}"))
}
fn initialize_inner(client: &mut PgDb) -> Result<()> {
    let exists=client.raw_query("SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname='oxide_internal'",&[])?;
    if let Some(row) = exists.first() {
        let version: Option<String> = row.get(0);
        if !matches!(
            version.as_deref(),
            Some("oxide:change-journal:v1") | Some(VERSION)
        ) {
            bail!("refusing to reuse an unowned change journal schema");
        }
    }
    client.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS oxide_internal; COMMENT ON SCHEMA oxide_internal IS '{VERSION}';
        CREATE TABLE IF NOT EXISTS oxide_internal.change_meta (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),epoch text NOT NULL,floor bigint NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS oxide_internal.changes (id bigserial PRIMARY KEY,db text NOT NULL,collection text NOT NULL,operation text NOT NULL,document jsonb,previous jsonb,created timestamptz NOT NULL DEFAULT clock_timestamp());
        ALTER TABLE oxide_internal.changes ADD COLUMN IF NOT EXISTS source_lsn text;
        CREATE UNIQUE INDEX IF NOT EXISTS changes_source_lsn ON oxide_internal.changes(source_lsn);
        CREATE INDEX IF NOT EXISTS changes_namespace ON oxide_internal.changes(db,collection,id);
        CREATE OR REPLACE FUNCTION oxide_internal.capture_change() RETURNS trigger LANGUAGE plpgsql AS $oxide$
        BEGIN
            PERFORM pg_logical_emit_message(true,'oxide.change.v2',jsonb_build_object('db',TG_TABLE_SCHEMA,'collection',TG_TABLE_NAME,'operation',TG_OP,'document',CASE WHEN TG_OP='DELETE' THEN NULL ELSE NEW._jsonb END,'previous',CASE WHEN TG_OP='INSERT' THEN NULL ELSE OLD._jsonb END)::text);
            RETURN NULL;
        END $oxide$;"))?;
    client.exec("INSERT INTO oxide_internal.change_meta(singleton,epoch) VALUES(true,$1) ON CONFLICT DO NOTHING",&[&bson::oid::ObjectId::new().to_hex()])?;
    for db in client.get_schemas()? {
        for collection in client.get_tables(&db)? {
            install_existing(client, &SqlParam::new(&db, &collection))?;
        }
    }
    Ok(())
}
pub fn trigger_sql(sp: &SqlParam) -> String {
    format!("CREATE TRIGGER oxide_change_capture AFTER INSERT OR UPDATE OR DELETE ON {sp} FOR EACH ROW EXECUTE FUNCTION oxide_internal.capture_change();")
}
pub fn install_existing(client: &mut PgDb, sp: &SqlParam) -> Result<()> {
    client.batch_execute(&format!("DROP TRIGGER IF EXISTS oxide_change_lock ON {sp}"))?;
    let exists:bool=client.raw_query("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=$1::text::regclass AND tgname='oxide_change_capture')",&[&sp.sanitize()])?[0].get(0);
    if !exists {
        client.batch_execute(&trigger_sql(sp))?;
    }
    Ok(())
}
/// Decode only pgoutput v1 transaction boundaries and our transactional messages.
/// The publication contains no tables; no tuple/DDL payloads are accepted.
fn message(data: &[u8]) -> Result<Option<(String, serde_json::Value)>> {
    if data.first() != Some(&b'M') {
        return Ok(None);
    }
    if data.len() < 15 || data[1] != 1 {
        bail!("invalid transactional logical message");
    }
    let lsn = u64::from_be_bytes(data[2..10].try_into()?);
    let end = data[10..]
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| eyre::eyre!("invalid logical message prefix"))?
        + 10;
    let prefix = std::str::from_utf8(&data[10..end])?;
    let length = data
        .get(end + 1..end + 5)
        .ok_or_else(|| eyre::eyre!("truncated logical message"))?;
    let length = u32::from_be_bytes(length.try_into()?) as usize;
    let content = data
        .get(end + 5..)
        .ok_or_else(|| eyre::eyre!("truncated logical message content"))?;
    if content.len() != length {
        bail!("invalid logical message length");
    }
    if prefix != "oxide.change.v2" {
        return Ok(None);
    }
    Ok(Some((
        format!("{:X}/{:X}", lsn >> 32, lsn & 0xffffffff),
        serde_json::from_slice(content)?,
    )))
}
pub fn pump(client: &mut PgDb) -> Result<()> {
    let locked: bool = client.raw_query("SELECT pg_try_advisory_lock($1)", &[&LOCK])?[0].get(0);
    if !locked {
        return Ok(());
    }
    let result = pump_locked(client).map(|_| ());
    let unlock = client.raw_query("SELECT pg_advisory_unlock($1)", &[&LOCK]);
    result?;
    unlock?;
    Ok(())
}
fn pump_locked(client: &mut PgDb) -> Result<bool> {
    let slot = slot_name(client)?;
    let rows=client.bounded_logical_query("SELECT lsn::text,data,octet_length(data) FROM pg_logical_slot_peek_binary_changes($1,NULL,1000,'proto_version','1','publication_names','oxide_changes_v2','messages','true')",&[&slot])?;
    let progress = !rows.is_empty();
    let mut pending = Vec::new();
    let mut acknowledged = None;
    for row in rows {
        let data: Vec<u8> = row.get(1);
        match data.first() {
            Some(b'B') => {
                if !pending.is_empty() {
                    bail!("unexpected logical transaction boundary");
                }
            }
            Some(b'M') => {
                if let Some(value) = message(&data)? {
                    pending.push(value);
                }
            }
            Some(b'C') => {
                if data.len() != 26 {
                    bail!("invalid logical commit message");
                }
                let timestamp = i64::from_be_bytes(data[18..26].try_into()?) / 1000 + 946684800000;
                client.batch_execute("BEGIN")?;
                let result = (|| -> Result<()> {
                    for (lsn, value) in pending.drain(..) {
                        let db = value["db"]
                            .as_str()
                            .ok_or_else(|| eyre::eyre!("missing change database"))?;
                        let collection = value["collection"]
                            .as_str()
                            .ok_or_else(|| eyre::eyre!("missing change collection"))?;
                        let operation = value["operation"]
                            .as_str()
                            .ok_or_else(|| eyre::eyre!("missing change operation"))?;
                        client.exec("INSERT INTO oxide_internal.changes(db,collection,operation,document,previous,created,source_lsn) VALUES($1,$2,$3,$4,$5,to_timestamp($6::double precision/1000),$7) ON CONFLICT(source_lsn) DO NOTHING",&[&db,&collection,&operation,&value["document"],&value["previous"],&(timestamp as f64),&lsn])?;
                    }
                    Ok(())
                })();
                if result.is_ok() {
                    client.batch_execute("COMMIT")?;
                } else {
                    let _ = client.batch_execute("ROLLBACK");
                    result?;
                }
                let end = u64::from_be_bytes(data[10..18].try_into()?);
                acknowledged = Some(format!("{:X}/{:X}", end >> 32, end & 0xffffffff));
            }
            _ => bail!("unexpected pgoutput message; Oxide publication must contain no tables"),
        }
    }
    if let Some(lsn) = acknowledged {
        client.raw_query(
            "SELECT end_lsn::text FROM pg_replication_slot_advance($1,$2::text::pg_lsn)",
            &[&slot, &lsn],
        )?;
    }
    Ok(progress)
}

/// Establish a stream's starting point after publishing the committed backlog.
/// The publisher lock orders concurrent opens; application writers never take it.
fn drain(client: &mut PgDb) -> Result<()> {
    client.raw_query("SELECT pg_advisory_lock($1)", &[&LOCK])?;
    let result = (|| -> Result<()> {
        let start = Instant::now();
        while pump_locked(client)? {
            if start.elapsed() > Duration::from_secs(25) {
                bail!("change stream initialization exceeds backlog budget");
            }
        }
        Ok(())
    })();
    let unlock = client.raw_query("SELECT pg_advisory_unlock($1)", &[&LOCK]);
    result?;
    unlock?;
    Ok(())
}

fn failure(message: &str, code: i32, name: &str) -> CommandExecutionError {
    CommandExecutionError {
        message: message.into(),
        code,
        code_name: name.into(),
    }
}
fn token(epoch: &str, id: i64) -> Document {
    doc! {"_data":format!("oxide1:{epoch}:{id}")}
}
fn bounds(client: &mut PgDb) -> Result<(String, i64, i64)> {
    let row=&client.raw_query("SELECT epoch,floor,GREATEST(floor,COALESCE((SELECT MAX(id) FROM oxide_internal.changes),0)) FROM oxide_internal.change_meta WHERE singleton",&[])?[0];
    Ok((row.get(0), row.get(1), row.get(2)))
}
pub fn open(request: &Request, command: &Document) -> Result<Document, CommandExecutionError> {
    check_options(command, &["aggregate", "pipeline", "cursor", "maxTimeMS"])?;
    if request.in_transaction() {
        return Err(CommandExecutionError::new(
            "change streams cannot run in a transaction".into(),
        ));
    }
    let sp = SqlParam::from(command, "aggregate")?;
    let pipeline = command.get_array("pipeline")?;
    if pipeline[0].as_document().is_none_or(|s| s.len() != 1) {
        return Err(CommandExecutionError::new(
            "pipeline stage requires exactly one operator".into(),
        ));
    }
    let options = pipeline[0]
        .as_document()
        .and_then(|s| s.get_document("$changeStream").ok())
        .ok_or_else(|| CommandExecutionError::new("invalid $changeStream".into()))?;
    check_options(options, &["fullDocument", "resumeAfter"])?;
    let lookup = match options.get_str("fullDocument") {
        Ok("updateLookup") => true,
        Ok("default") | Err(bson::document::ValueAccessError::NotPresent) => false,
        _ => {
            return Err(CommandExecutionError::new(
                "unsupported fullDocument mode".into(),
            ))
        }
    };
    // Compile the follow-on pipeline now, including an empty stream's validation.
    crate::commands::compile_sql(&sp, &pipeline[1..])?;
    let mut client = request.get_client()?;
    drain(&mut client)?;
    let (epoch, floor, latest) = bounds(&mut client)?;
    let last = if let Some(resume) = options.get("resumeAfter") {
        let value = resume
            .as_document()
            .and_then(|d| d.get_str("_data").ok())
            .ok_or_else(|| failure("invalid resume token", 260, "InvalidResumeToken"))?;
        let prefix = format!("oxide1:{epoch}:");
        let last = value
            .strip_prefix(&prefix)
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|id| *id >= 0 && *id <= latest)
            .ok_or_else(|| failure("invalid resume token", 260, "InvalidResumeToken"))?;
        if last < floor {
            return Err(failure(
                "resume token is outside journal retention",
                286,
                "ChangeStreamHistoryLost",
            ));
        }
        last
    } else {
        latest
    };
    let batch = cursor_batch_size(command)?;
    let mut stream = Stream {
        namespace: format!("{}.{}", sp.db, sp.collection),
        touched: Instant::now(),
        last,
        epoch,
        lookup,
        pipeline: pipeline[1..].to_vec(),
    };
    let documents = read(&mut client, &sp, &mut stream, batch)?;
    let mut cursors = request
        .cursors
        .lock()
        .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?;
    let id = cursors.add_stream(stream.clone())?;
    Ok(
        doc! {"ok":1.0,"cursor":{"id":id,"ns":stream.namespace,"firstBatch":documents,"postBatchResumeToken":token(&stream.epoch,stream.last)}},
    )
}
fn read(
    client: &mut PgDb,
    sp: &SqlParam,
    stream: &mut Stream,
    batch: usize,
) -> Result<Vec<Document>> {
    pump(client)?;
    let (_, floor, latest) = bounds(client)?;
    if stream.last < floor {
        bail!("change stream history lost");
    }
    if batch == 0 {
        return Ok(Vec::new());
    }
    let rows=client.raw_query("SELECT id,operation,document,previous,(extract(epoch from created)*1000)::bigint FROM oxide_internal.changes WHERE id>$1 AND db=$2 AND collection=$3 ORDER BY id LIMIT $4",&[&stream.last,&sp.db,&sp.collection,&(batch.min(1000) as i64)])?;
    let mut events = Vec::new();
    let mut bytes = 0;
    for row in &rows {
        let id: i64 = row.get(0);
        let operation: String = row.get(1);
        let new: Option<serde_json::Value> = row.get(2);
        let old: Option<serde_json::Value> = row.get(3);
        let time: i64 = row.get(4);
        let new = new
            .map(|j| j.to_bson())
            .transpose()?
            .and_then(|b| b.as_document().cloned());
        let old = old
            .map(|j| j.to_bson())
            .transpose()?
            .and_then(|b| b.as_document().cloned());
        let key = new
            .as_ref()
            .or(old.as_ref())
            .and_then(|d| d.get("_id"))
            .cloned()
            .ok_or_else(|| eyre::eyre!("journal document has no identity"))?;
        let mut event = doc! {"_id":token(&stream.epoch,id),"operationType":match operation.as_str(){"INSERT"=>"insert","DELETE"=>"delete",_=>"update"},"ns":{"db":&sp.db,"coll":&sp.collection},"documentKey":{"_id":key.clone()},"clusterTime":bson::Timestamp{time:(time/1000) as u32,increment:id as u32},"wallTime":bson::DateTime::from_millis(time)};
        if operation == "INSERT" {
            event.insert("fullDocument", new.clone());
        }
        if operation == "UPDATE" {
            let before = old
                .as_ref()
                .ok_or_else(|| eyre::eyre!("missing journal preimage"))?;
            let after = new
                .as_ref()
                .ok_or_else(|| eyre::eyre!("missing journal postimage"))?;
            let updated: Document = after
                .iter()
                .filter(|(k, v)| before.get(k.as_str()) != Some(v))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let removed: Vec<_> = before
                .keys()
                .filter(|k| !after.contains_key(k.as_str()))
                .cloned()
                .collect();
            event.insert(
                "updateDescription",
                doc! {"updatedFields":updated,"removedFields":removed,"truncatedArrays":[]},
            );
            if stream.lookup {
                let current = if client.table_exists(&sp.db, &sp.collection)? {
                    client
                        .find(sp, &doc! {"_id":key}, None, 0, 1)?
                        .into_iter()
                        .next()
                } else {
                    None
                };
                event.insert(
                    "fullDocument",
                    current.map(Bson::Document).unwrap_or(Bson::Null),
                );
            }
        }
        let size = bson::to_vec(&event)?.len();
        if size > crate::pg::MAX_QUERY_BYTES {
            bail!("change event exceeds byte limit");
        }
        if bytes + size > crate::pg::MAX_QUERY_BYTES {
            break;
        }
        bytes += size;
        events.push(event);
        stream.last = id;
    }
    if events.len() == rows.len() && rows.len() < batch.min(1000) {
        stream.last = stream.last.max(latest);
    }
    if !stream.pipeline.is_empty() && !events.is_empty() {
        client.prepare_aggregation()?;
        let mut sql = crate::sql::Sql::default();
        let input = sql.bind(
            Bson::Array(events.into_iter().map(Bson::Document).collect())
                .into_psql_json()
                .to_string(),
            "jsonb",
        );
        let source = crate::commands::compile_stream_pipeline(
            sp,
            &stream.pipeline,
            &mut sql,
            format!("SELECT value AS _jsonb FROM jsonb_array_elements({input}) AS events(value)"),
        )?;
        sql.text = source;
        events = client.documents(&sql, None)?;
    }
    stream.touched = Instant::now();
    Ok(events)
}
pub fn more(
    request: &Request,
    command: &Document,
    id: i64,
    ns: &str,
    batch: usize,
) -> Result<Option<Document>, CommandExecutionError> {
    let stream = request
        .cursors
        .lock()
        .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
        .get_stream(id, ns)?;
    let Some(mut stream) = stream else {
        return Ok(None);
    };
    if request.in_transaction() {
        return Err(CommandExecutionError::new(
            "change streams cannot run in transactions".into(),
        ));
    }
    let sp = SqlParam::from(command, "collection")?;
    let wait = command
        .get("maxTimeMS")
        .map(crate::pg::integer)
        .transpose()?
        .unwrap_or(1000);
    if wait < 0 {
        return Err(CommandExecutionError::new(
            "maxTimeMS must be nonnegative".into(),
        ));
    }
    let deadline = Instant::now() + Duration::from_millis(wait.min(1000) as u64);
    let mut client = request.get_client()?;
    let events = loop {
        let events = read(&mut client, &sp, &mut stream, batch).map_err(|e| {
            if e.to_string() == "change stream history lost" {
                failure(
                    "resume token is outside journal retention",
                    286,
                    "ChangeStreamHistoryLost",
                )
            } else {
                e.into()
            }
        })?;
        if !events.is_empty() || Instant::now() >= deadline {
            break events;
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    request
        .cursors
        .lock()
        .map_err(|_| CommandExecutionError::new("cursor lock unavailable".into()))?
        .update_stream(id, stream.clone())?;
    Ok(Some(
        doc! {"ok":1.0,"cursor":{"id":id,"ns":ns,"nextBatch":events,"postBatchResumeToken":token(&stream.epoch,stream.last)}},
    ))
}
/// Keep 24 hours and at most 100,000 entries. Advancing the floor makes stale
/// resume tokens fail explicitly instead of silently missing invalidations.
pub fn prune(client: &mut PgDb) -> Result<()> {
    client.exec("WITH removed AS (DELETE FROM oxide_internal.changes WHERE created<clock_timestamp()-interval '24 hours' OR id<=(SELECT COALESCE(MAX(id),0)-100000 FROM oxide_internal.changes) RETURNING id) UPDATE oxide_internal.change_meta SET floor=GREATEST(floor,COALESCE((SELECT MAX(id) FROM removed),0)) WHERE singleton",&[])?;
    Ok(())
}
