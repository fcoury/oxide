use deno_core::{
    error::AnyError,
    op,
    v8::{self},
    Extension, JsRuntime,
};
use mongodb::{
    bson::Document,
    options::ClientOptions,
    sync::{Client, Collection},
};
use serde_json::Value;
use std::rc::Rc;

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

async fn _run_js(file_path: &str) -> Result<(), AnyError> {
    let main_module = deno_core::resolve_path(file_path)?;
    let extension = Extension::builder().ops(vec![op_find::decl()]).build();
    let mut js_runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions {
        module_loader: Some(Rc::new(deno_core::FsModuleLoader)),
        extensions: vec![extension],
        ..Default::default()
    });
    js_runtime
        .execute_script("[runjs:runtime.js]", include_str!("./runtime.js"))
        .unwrap();

    let mod_id = js_runtime.load_main_module(&main_module, None).await?;
    let result = js_runtime.mod_evaluate(mod_id);
    js_runtime.run_event_loop(false).await?;
    result.await?
}

async fn run_repl(addr: &str, port: u16) -> Result<(), AnyError> {
    let extension = Extension::builder().ops(vec![op_find::decl()]).build();
    let mut js_runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions {
        module_loader: Some(Rc::new(deno_core::FsModuleLoader)),
        extensions: vec![extension],
        ..Default::default()
    });

    let const_str = &format!(
        r#"((globalThis) => {{ globalThis._state = {{ dbAddr: "{}", dbPort: {}, db: "test" }}; }})(globalThis);"#,
        addr, port
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
    loop {
        let db = eval(&mut js_runtime, "_state?.db").unwrap();
        let prompt = format!("{}> ", db.as_str().unwrap());
        let line = rl.readline(&prompt)?;
        rl.add_history_entry(&line);
        rl.save_history(&history).unwrap();

        match js_runtime.execute_script("[runjs:repl]", &line) {
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

pub fn start(addr: &str, port: u16) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Err(error) = runtime.block_on(run_repl(addr, port)) {
        eprintln!("error: {}", error);
    }
}
