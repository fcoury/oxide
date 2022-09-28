use colored::*;
use deno_core::{
    error::AnyError,
    op,
    v8::{self},
    Extension, JsRuntime,
};
use mongodb::{
    bson::Document,
    options::ClientOptions,
    sync::{Client, Collection, Database},
};
use serde_json::Value;
use std::fs;
use std::rc::Rc;

pub struct Shell {
    db_host: String,
    db_port: u16,
}

impl Shell {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            db_host: host.to_string(),
            db_port: port,
        }
    }

    pub fn start(&self) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        if let Err(error) = runtime.block_on(self.run_repl()) {
            eprintln!("error: {}", error);
        }
    }

    async fn run_repl(&self) -> Result<(), AnyError> {
        let extension = Extension::builder()
            .ops(vec![
                op_find::decl(),
                op_insert_one::decl(),
                op_insert_many::decl(),
                op_update_one::decl(),
                op_update_many::decl(),
                op_delete_one::decl(),
                op_delete_many::decl(),
                op_aggregate::decl(),
                op_drop::decl(),
                op_save::decl(),
                op_list_collections::decl(),
            ])
            .build();
        let mut js_runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions {
            module_loader: Some(Rc::new(deno_core::FsModuleLoader)),
            extensions: vec![extension],
            ..Default::default()
        });

        let const_str = &format!(
            r#"((globalThis) => {{ globalThis._state = {{ dbAddr: "{}", dbPort: {}, db: "test" }}; }})(globalThis);"#,
            self.db_host, self.db_port
        );
        js_runtime
            .execute_script("[runjs:const.js]", const_str)
            .unwrap();
        js_runtime
            .execute_script("[runjs:runtime.js]", include_str!("./runtime.js"))
            .unwrap();

        let mut rl = rustyline::Editor::<()>::new()?;
        let history = format!(
            "{}/.oxide_history",
            dirs::home_dir().unwrap().as_os_str().to_str().unwrap()
        );
        if rl.load_history(&history).is_err() {
            eprintln!("Couldn't load history file: {}", history);
        };
        let state = eval(&mut js_runtime, "_state").unwrap();
        println!("{} Shell", "OxideDB".yellow());
        println!(
            "Connecting to: mongodb://{}:{}",
            state.get("dbAddr").unwrap().as_str().unwrap(),
            state.get("dbPort").unwrap()
        );
        println!("");

        let mut allow_break = false;
        loop {
            let db = eval(&mut js_runtime, "_state?.db").unwrap();
            let prompt = format!("{}> ", db.as_str().unwrap());
            let line = rl.readline(&prompt);
            match line {
                Ok(line) => {
                    let mut source = line.clone();
                    let mut file_name = "[oxidedb:shell]".to_string();
                    allow_break = false;

                    rl.add_history_entry(&line);
                    rl.save_history(&history).unwrap();

                    let tr_line = line.trim();
                    if tr_line.is_empty() {
                        continue;
                    }

                    if tr_line == "exit" {
                        break;
                    }

                    if tr_line == "show collections" {
                        let db = eval(&mut js_runtime, "db").unwrap();
                        self.show_collections(&db)?;
                        continue;
                    }

                    if tr_line == "show databases" {
                        let db = eval(&mut js_runtime, "db").unwrap();
                        self.show_databases(&db)?;
                        continue;
                    }

                    if tr_line.starts_with("use ") {
                        let db = tr_line.split(" ").nth(1).unwrap();
                        let cmd = &format!(r#"use("{}")"#, db.to_string());
                        js_runtime.execute_script(&file_name, cmd).unwrap();
                        continue;
                    }

                    if tr_line.starts_with("run ") {
                        let file = tr_line.split_once(" ").unwrap().1;
                        let contents = fs::read_to_string(file);
                        match contents {
                            Ok(src) => {
                                file_name = file.to_owned();
                                source = src.clone();
                            }
                            Err(error) => {
                                println!("Error running {}: {}", file, error);
                                continue;
                            }
                        };
                    }

                    match js_runtime.execute_script(&file_name, &source) {
                        Ok(value) => {
                            js_runtime.run_event_loop(false).await?;

                            let scope = &mut js_runtime.handle_scope();
                            let local = v8::Local::new(scope, value);
                            let deserialized_value = serde_v8::from_v8::<Value>(scope, local);
                            if let Ok(value) = deserialized_value {
                                println!("{}", serde_json::to_string_pretty(&value).unwrap());
                            }
                        }
                        Err(err) => {
                            eprintln!("{}", err.to_string())
                        }
                    }
                }
                Err(rustyline::error::ReadlineError::Interrupted) => {
                    if allow_break {
                        break;
                    }
                    allow_break = true;
                }
                Err(rustyline::error::ReadlineError::Eof) => {
                    break;
                }
                Err(err) => {
                    println!("Error: {:?}", err);
                    break;
                }
            }
        }
        Ok(())
    }

    fn show_databases(&self, db: &Value) -> Result<(), AnyError> {
        let db = client(db.clone())?;
        let cursor = db.list_databases(None, None)?;
        cursor.iter().for_each(|doc| {
            println!("{}", doc.name.bold());
        });
        Ok(())
    }

    fn show_collections(&self, db: &Value) -> Result<(), AnyError> {
        let db = database(db.clone())?;
        let cursor = db.list_collections(None, None)?;
        let res = cursor.collect::<Vec<_>>();
        res.iter().for_each(|doc| {
            let doc = doc.clone().unwrap();
            println!("{}", doc.name.bold());
        });
        Ok(())
    }
}

fn client(db: Value) -> Result<Client, mongodb::error::Error> {
    let db_obj = db.as_object().unwrap();
    let db_host = db_obj.get("addr").unwrap().as_str().unwrap();
    let db_port = db_obj.get("port").unwrap().as_u64().unwrap();
    let db_name = db_obj.get("name").unwrap().as_str().unwrap();

    let client_uri = format!("mongodb://{db_host}:{db_port}/{db_name}");
    let client_options = ClientOptions::parse(&client_uri).unwrap();
    Client::with_options(client_options)
}

fn database(db: Value) -> Result<Database, AnyError> {
    let db_obj = db.as_object().unwrap();

    let db_host = db_obj.get("addr").unwrap().as_str().unwrap();
    let db_port = db_obj.get("port").unwrap().as_u64().unwrap();
    let db_name = db_obj.get("name").unwrap().as_str().unwrap();

    let client_uri = format!("mongodb://{db_host}:{db_port}/{db_name}");
    let client_options = ClientOptions::parse(&client_uri).unwrap();
    let client = Client::with_options(client_options)?;

    let db = client.database(db_name);

    Ok(db)
}

fn collection(col: Value) -> Result<Collection<Document>, AnyError> {
    let col_obj = col.as_object().unwrap();
    let db_obj = col_obj.get("db").unwrap().as_object().unwrap();

    let db_host = db_obj.get("addr").unwrap().as_str().unwrap();
    let db_name = db_obj.get("name").unwrap().as_str().unwrap();
    let db_port = db_obj.get("port").unwrap().as_u64().unwrap();
    let col_name = col_obj.get("name").unwrap().as_str().unwrap();

    let client_uri = format!("mongodb://{db_host}:{db_port}/{db_name}");
    let client_options = ClientOptions::parse(&client_uri).unwrap();
    let client = Client::with_options(client_options)?;

    let db = client.database(db_name);
    let col: Collection<Document> = db.collection(col_name);

    Ok(col)
}

#[op]
fn op_find(col: Value, filter: Value) -> Result<Vec<Value>, AnyError> {
    let col = collection(col)?;
    let filter = bson::ser::to_bson(&filter).unwrap();
    let filter = filter.as_document().unwrap();
    let cursor = col.find(filter.clone(), None).unwrap();
    let res = cursor.collect::<Vec<_>>();
    let res = res
        .iter()
        .map(|doc| {
            let doc = doc.as_ref().unwrap();
            serde_json::to_value(doc).unwrap()
        })
        .collect::<Vec<_>>();
    Ok(res)
}

#[op]
fn op_insert_one(col: Value, doc: Value) -> Result<Value, AnyError> {
    let col = collection(col)?;
    let doc = bson::ser::to_bson(&doc).unwrap();
    let doc = doc.as_document().unwrap();
    let res = col.insert_one(doc.clone(), None).unwrap();
    let res = serde_json::to_value(&res).unwrap();
    Ok(res)
}

#[op]
fn op_insert_many(col: Value, docs: Value) -> Result<Value, AnyError> {
    let col = collection(col)?;
    let docs = docs.as_array().unwrap();
    let docs = docs
        .iter()
        .map(|doc| {
            let doc = bson::ser::to_bson(&doc).unwrap();
            let doc = doc.as_document().unwrap();
            doc.clone()
        })
        .collect::<Vec<_>>();
    let res = col.insert_many(docs, None).unwrap();
    let res = serde_json::to_value(&res).unwrap();
    Ok(res)
}

#[op]
fn op_delete_one(col: Value, filter: Value) -> Result<Value, AnyError> {
    let col = collection(col)?;
    let filter = bson::ser::to_bson(&filter).unwrap();
    let filter = filter.as_document().unwrap();
    let res = col.delete_one(filter.clone(), None).unwrap();
    let res = serde_json::to_value(&res).unwrap();
    Ok(res)
}

#[op]
fn op_delete_many(col: Value, filter: Value) -> Result<Value, AnyError> {
    let col = collection(col)?;
    let filter = bson::ser::to_bson(&filter).unwrap();
    let filter = filter.as_document().unwrap();
    let res = col.delete_many(filter.clone(), None).unwrap();
    let res = serde_json::to_value(&res).unwrap();
    Ok(res)
}

#[op]
fn op_update_one(col: Value, filter: Value, update: Value) -> Result<Value, AnyError> {
    let col = collection(col)?;
    let filter = bson::ser::to_bson(&filter).unwrap();
    let filter = filter.as_document().unwrap();
    let update = bson::ser::to_bson(&update).unwrap();
    let update = update.as_document().unwrap();
    let res = col
        .update_one(filter.clone(), update.clone(), None)
        .unwrap();
    let res = serde_json::to_value(&res).unwrap();
    Ok(res)
}

#[op]
fn op_update_many(col: Value, filter: Value, update: Value) -> Result<Value, AnyError> {
    let col = collection(col)?;
    let filter = bson::ser::to_bson(&filter).unwrap();
    let filter = filter.as_document().unwrap();
    let update = bson::ser::to_bson(&update).unwrap();
    let update = update.as_document().unwrap();
    let res = col
        .update_many(filter.clone(), update.clone(), None)
        .unwrap();
    let res = serde_json::to_value(&res).unwrap();
    Ok(res)
}

#[op]
fn op_aggregate(col: Value, pipeline: Value) -> Result<Vec<Value>, AnyError> {
    let col = collection(col)?;
    let pipeline = pipeline.as_array().unwrap();
    let pipeline = pipeline
        .iter()
        .map(|doc| {
            let doc = bson::ser::to_bson(&doc).unwrap();
            let doc = doc.as_document().unwrap();
            doc.clone()
        })
        .collect::<Vec<_>>();
    let cursor = col.aggregate(pipeline, None).unwrap();
    let res = cursor.collect::<Vec<_>>();
    let res = res
        .iter()
        .map(|doc| {
            let doc = doc.as_ref().unwrap();
            serde_json::to_value(doc).unwrap()
        })
        .collect::<Vec<_>>();
    Ok(res)
}

#[op]
fn op_drop(col: Value) -> Result<Value, AnyError> {
    let col = collection(col)?;
    col.drop(None).unwrap();
    Ok(Value::Bool(true))
}

#[op]
fn op_save(col: Value, doc: Value) -> Result<Value, AnyError> {
    let col = collection(col)?;
    let id = doc.get("_id");
    let doc = bson::ser::to_bson(&doc).unwrap();
    let doc = doc.as_document().unwrap();
    let res = match id {
        Some(id) => {
            let id = id.as_str().unwrap();
            serde_json::to_value(
                col.update_one(bson::doc! { "id": id }, doc.clone(), None)
                    .unwrap(),
            )
            .unwrap()
        }
        None => serde_json::to_value(col.insert_one(doc.clone(), None).unwrap()).unwrap(),
    };
    Ok(res)
}

#[op]
fn op_list_collections(db: Value) -> Result<Vec<Value>, AnyError> {
    let db = database(db)?;
    let cursor = db.list_collections(None, None).unwrap();
    let res = cursor.collect::<Vec<_>>();
    let res = res
        .iter()
        .map(|doc| {
            let doc = doc.clone().unwrap();
            serde_json::to_value(&doc).unwrap()
        })
        .collect::<Vec<_>>();
    Ok(res)
}

fn eval(context: &mut JsRuntime, code: &str) -> Result<Value, String> {
    let res = context.execute_script("<anon>", code);
    match res {
        Ok(global) => {
            let scope = &mut context.handle_scope();
            let local = v8::Local::new(scope, global);
            // Deserialize a `v8` object into a Rust type using `serde_v8`,
            // in this case deserialize to a JSON `Value`.
            let deserialized_value = serde_v8::from_v8::<Value>(scope, local);

            match deserialized_value {
                Ok(value) => Ok(value),
                Err(err) => Err(format!("Cannot deserialize value: {:?}", err)),
            }
        }
        Err(err) => Err(format!("Evaling error: {:?}", err)),
    }
}
