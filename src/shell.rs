//! Use the maintained MongoDB shell instead of embedding a second JavaScript runtime.
use eyre::{bail, Result, WrapErr};
use std::process::Command;

pub fn start(host: &str, port: u16) -> Result<()> {
    let status = Command::new("mongosh")
        .args(["--host", host, "--port", &port.to_string()])
        .status()
        .wrap_err("cannot launch mongosh; install MongoDB Shell and put mongosh on PATH")?;
    if !status.success() {
        bail!("mongosh exited with {status}");
    }
    Ok(())
}
