//! Optional, embedded JavaScript shell. Database access always uses the wire protocol.
mod bson;
mod editor;
mod transport;

use eyre::{eyre, Result, WrapErr};
use mongodb::options::{ClientOptions, ServerAddress};
use rquickjs::{CatchResultExt, Context, Ctx, Function, Runtime, Value};
use rustyline::error::ReadlineError;
use std::rc::Rc;
use std::sync::Arc;
use std::{
    io::{self, BufRead, IsTerminal},
    time::Duration,
};
use tokio::sync::watch;

/// A persistent JavaScript session backed by one MongoDB client pool.
pub struct Shell {
    context: Context,
    runtime: Runtime,
    interrupted: watch::Sender<bool>,
    transport: Rc<transport::Transport>,
    initial_database: String,
    output: Output,
}

#[derive(Clone)]
enum Output {
    Json,
    Interactive {
        syntax: Arc<editor::Syntax>,
        color: bool,
    },
}
impl Output {
    fn render(&self, text: &str) -> String {
        match self {
            Self::Interactive {
                syntax,
                color: true,
            } => syntax.highlight(text, None),
            _ => text.into(),
        }
    }
}

impl Shell {
    pub fn new(options: ClientOptions) -> Result<Self> {
        Self::with_output(options, Output::Json)
    }

    fn with_output(options: ClientOptions, output: Output) -> Result<Self> {
        let database = options
            .default_database
            .clone()
            .unwrap_or_else(|| "test".into());
        let (interrupted, receiver) = watch::channel(false);
        let transport = Rc::new(transport::Transport::new(options, receiver)?);
        Self::with_transport(transport, interrupted, database, output)
    }

    fn with_transport(
        transport: Rc<transport::Transport>,
        interrupted: watch::Sender<bool>,
        initial_database: String,
        output: Output,
    ) -> Result<Self> {
        let runtime = Runtime::new()?;
        runtime.set_memory_limit(64 * 1024 * 1024);
        runtime.set_max_stack_size(512 * 1024);
        let context = Context::full(&runtime)?;
        context.with(|ctx| -> Result<()> {
            let transport = transport.clone();
            ctx.globals().set(
                "__oxideCommand",
                Function::new(
                    ctx.clone(),
                    move |ctx: Ctx<'_>, database: String, command: bson::CommandDocument| {
                        transport
                            .command(&database, command.0)
                            .map(bson::CommandDocument)
                            .map_err(|e| bson::error(&ctx, e))
                    },
                )?,
            )?;
            ctx.globals()
                .set("__oxideSize", Function::new(ctx.clone(), bson::size)?)?;
            ctx.globals()
                .set("__oxideScalar", Function::new(ctx.clone(), bson::scalar)?)?;
            let interactive = matches!(output, Output::Interactive { .. });
            ctx.globals().set("__oxideInteractive", interactive)?;
            if interactive {
                ctx.globals().set(
                    "__oxideInspectScalar",
                    Function::new(ctx.clone(), bson::inspect_scalar)?,
                )?;
                ctx.eval::<(), _>(include_str!("inspect.js"))
                    .catch(&ctx)
                    .map_err(|e| eyre!(e.to_string()))?;
            }
            let printer = output.clone();
            ctx.globals().set(
                "__oxidePrint",
                Function::new(ctx.clone(), move |line: String| {
                    println!("{}", printer.render(&line))
                })?,
            )?;
            ctx.eval::<(), _>(include_str!("runtime.js"))
                .catch(&ctx)
                .map_err(|e| eyre!(e.to_string()))?;
            ctx.globals()
                .get::<_, Function>("use")?
                .call::<_, Value>((initial_database.as_str(),))
                .catch(&ctx)
                .map_err(|e| eyre!(e.to_string()))?;
            Ok(())
        })?;
        // Bootstrap contains only our code. A second Ctrl-C during recovery
        // must not prevent the fresh interpreter from replacing the old one.
        let signal = interrupted.clone();
        runtime.set_interrupt_handler(Some(Box::new(move || *signal.borrow())));
        Ok(Self {
            context,
            runtime,
            interrupted,
            transport,
            initial_database,
            output,
        })
    }

    /// Evaluate one command, retaining variables after ordinary errors. If a
    /// failed command leaves Promise jobs, reset JavaScript to discard them.
    pub fn eval(&mut self, source: &str) -> Result<Option<String>> {
        // A failed allocation during the previous reset must never let stale
        // jobs resume. Retry initialization before accepting any more user code.
        if self.runtime.is_job_pending() {
            self.reset()?;
        }
        self.interrupted.send_replace(false);
        let result = self.context.with(|ctx| {
            let run = || -> rquickjs::Result<Option<String>> {
                let value: Value = if let Some(database) = source.trim().strip_prefix("use ") {
                    ctx.globals().get::<_, Function>("use")?.call((database.trim(),))?
                } else {
                    let source = match source.trim() {
                        "show collections" => "db.listCollections().map(c => c.name)",
                        "show databases" | "show dbs" => "db.getSiblingDB('admin').runCommand({listDatabases: 1}).databases.map(d => d.name)",
                        _ => source,
                    };
                    ctx.eval(source)?
                };
                let format: Function = ctx.globals().get("__oxideFormat")?;
                format.call((value,))
            };
            run().catch(&ctx).map_err(|e| eyre!(e.to_string()))
        }).and_then(|result| {
            while self.runtime.is_job_pending() {
                if *self.interrupted.borrow() { return Err(eyre!("interrupted")); }
                self.runtime.execute_pending_job().map_err(|e| e.0.with(|ctx| eyre!(rquickjs::CaughtError::from_error(&ctx, rquickjs::Error::Exception).to_string())))?;
            }
            Ok(result)
        });
        self.transport.close_cursors();
        self.interrupted.send_replace(false);
        if result.is_err() && self.runtime.is_job_pending() {
            // QuickJS cannot discard jobs individually. Running them under an
            // interrupt still permits short callbacks to enqueue more jobs.
            // Recreate only the interpreter, keeping the client and Ctrl-C handle.
            self.reset()?;
            return result.wrap_err(format!(
                "JavaScript session reset to discard pending Promise jobs; variables cleared and database reset to {}",
                self.initial_database,
            ));
        }
        result
    }

    fn reset(&mut self) -> Result<()> {
        *self = Self::with_transport(
            self.transport.clone(),
            self.interrupted.clone(),
            self.initial_database.clone(),
            self.output.clone(),
        )?;
        Ok(())
    }

    /// The caller may signal Ctrl-C without touching the JavaScript context.
    pub fn interrupt_handle(&self) -> watch::Sender<bool> {
        self.interrupted.clone()
    }

    pub fn database(&self) -> Result<String> {
        self.context.with(|ctx| {
            ctx.eval::<String, _>("db.getName()")
                .catch(&ctx)
                .map_err(|e| eyre!(e.to_string()))
        })
    }
}

pub fn start(
    host: &str,
    port: u16,
    database: &str,
    eval: Option<&str>,
    no_history: bool,
) -> Result<()> {
    let options = ClientOptions::builder()
        .hosts(vec![ServerAddress::Tcp {
            host: host.into(),
            port: Some(port),
        }])
        .direct_connection(true)
        .server_selection_timeout(Duration::from_secs(5))
        .connect_timeout(Duration::from_secs(5))
        .retry_reads(false)
        .retry_writes(false)
        .default_database(database.to_owned())
        .build();
    let interactive = eval.is_none() && io::stdin().is_terminal();
    let color = io::stdout().is_terminal()
        && std::env::var_os("NO_COLOR").is_none()
        && std::env::var("TERM").as_deref() != Ok("dumb");
    let output = if interactive {
        Output::Interactive {
            syntax: Arc::new(editor::Syntax::new()?),
            color,
        }
    } else {
        Output::Json
    };
    let mut shell = Shell::with_output(options, output.clone())?;
    // Rustyline installs a SIGINT handler when constructing its Unix terminal.
    // Install ours afterwards so Ctrl-C also cancels code between readline calls.
    let editor = match &output {
        Output::Interactive { syntax, color } => Some(editor::editor(syntax.clone(), *color)?),
        Output::Json => None,
    };
    let signal = shell.interrupt_handle();
    ctrlc::set_handler(move || {
        signal.send_replace(true);
    })
    .wrap_err("cannot install shell interrupt handler")?;
    if let Some(source) = eval {
        if let Some(output) = shell.eval(source)? {
            println!("{output}");
        }
        return Ok(());
    }
    if !io::stdin().is_terminal() {
        for line in io::stdin().lock().lines() {
            let line = line?;
            if line.trim() == "exit" {
                break;
            }
            if let Some(output) = shell.eval(&line)? {
                println!("{output}");
            }
        }
        return Ok(());
    }
    let mut editor = editor.expect("interactive input has an editor");
    let history = if no_history {
        None
    } else {
        dirs::home_dir().map(|dir| dir.join(".oxide_history"))
    };
    if let Some(history) = &history {
        if history.exists() {
            if let Err(e) = editor.load_history(history) {
                eprintln!("Cannot load shell history: {e}");
            }
        }
    }
    println!("OxideDB Shell — target mongodb://{host}:{port}/{database}\nTab completes; incomplete JavaScript continues on Enter. Ctrl-C cancels; Ctrl-D or exit leaves.");
    loop {
        let database = shell
            .database()
            .unwrap_or_else(|_| shell.initial_database.clone());
        if let Some(helper) = editor.helper_mut() {
            helper.refresh(&database, shell.transport.collection_names(&database));
        }
        let prompt = format!("{database}> ");
        match editor.readline(&prompt) {
            Ok(line) if line.trim() == "exit" => break,
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                editor.add_history_entry(line.as_str())?;
                match shell.eval(&line) {
                    Ok(Some(value)) => println!("{}", output.render(&value)),
                    Ok(None) => {}
                    Err(error) => eprintln!("{error}"),
                }
            }
            Err(ReadlineError::Interrupted) => continue,
            Err(ReadlineError::Eof) => break,
            Err(error) => return Err(error.into()),
        }
    }
    if let Some(history) = &history {
        if let Err(error) = editor.save_history(history) {
            eprintln!("Cannot save shell history: {error}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
