use deno_core::{error::AnyError, op, v8, Extension};
use std::rc::Rc;

#[op]
fn op_db(path: String) -> Result<String, AnyError> {
    println!("got: {}", path);
    Ok(path)
}

async fn _run_js(file_path: &str) -> Result<(), AnyError> {
    let main_module = deno_core::resolve_path(file_path)?;
    let extension = Extension::builder().ops(vec![op_db::decl()]).build();
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

async fn run_repl() -> Result<(), AnyError> {
    let extension = Extension::builder().ops(vec![op_db::decl()]).build();
    let mut js_runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions {
        module_loader: Some(Rc::new(deno_core::FsModuleLoader)),
        extensions: vec![extension],
        ..Default::default()
    });

    let mut rl = rustyline::Editor::<()>::new()?;
    let history = format!(
        "{}/.history",
        dirs::home_dir().unwrap().as_os_str().to_str().unwrap()
    );
    if rl.load_history(&history).is_err() {
        eprintln!("Couldn't load history file: {}", history);
    };
    loop {
        let line = rl.readline(">> ")?;
        rl.add_history_entry(&line);
        rl.save_history(&history).unwrap();

        js_runtime
            .execute_script("[runjs:runtime.js]", include_str!("./runtime.js"))
            .unwrap();
        match js_runtime.execute_script("[runjs:repl]", &line) {
            Ok(value) => {
                js_runtime.run_event_loop(false).await?;

                let scope = &mut js_runtime.handle_scope();
                let local = v8::Local::new(scope, value);
                let deserialized_value = serde_v8::from_v8::<serde_json::Value>(scope, local);
                if let Ok(value) = deserialized_value {
                    println!("{}", value)
                }
            }
            Err(err) => {
                eprintln!("{}", err.to_string())
            }
        }
    }
}

pub fn start() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Err(error) = runtime.block_on(run_repl()) {
        eprintln!("error: {}", error);
    }
}
