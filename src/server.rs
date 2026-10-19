use crate::{
    cursor::Cursors,
    handler::{handle_with_cursors, Response},
    threadpool::ThreadPool,
    wire::{self, OpCode},
};
use bson::doc;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use std::{
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
static NEXT_ID: AtomicU32 = AtomicU32::new(1);
pub struct Server {
    listen_addr: String,
    port: u16,
    pg_url: String,
}
impl Server {
    pub fn new(listen_addr: String, port: u16) -> Self {
        Self::new_with_pgurl(
            listen_addr,
            port,
            env::var("DATABASE_URL").unwrap_or_else(|_| "postgres://localhost:5432/oxide".into()),
        )
    }
    pub fn new_with_pgurl(listen_addr: String, port: u16, pg_url: String) -> Self {
        Self {
            listen_addr,
            port,
            pg_url,
        }
    }
    pub fn start(&self) {
        if let Err(error) = self.try_start() {
            log::error!("server startup failed: {error}");
        }
    }
    pub fn try_start(&self) -> eyre::Result<()> {
        let manager = PostgresConnectionManager::new(self.pg_url.parse()?, NoTls);
        let pool = r2d2::Pool::builder()
            .max_size(10)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)?;
        self.serve(
            TcpListener::bind((self.listen_addr.as_str(), self.port))?,
            pool,
        )
    }
    pub fn start_with_pool(&self, pool: r2d2::Pool<PostgresConnectionManager<NoTls>>) {
        if let Err(error) = TcpListener::bind((self.listen_addr.as_str(), self.port))
            .map_err(eyre::Report::from)
            .and_then(|listener| self.serve(listener, pool))
        {
            log::error!("server failed: {error}");
        }
    }
    pub fn serve(
        &self,
        listener: TcpListener,
        pg_pool: r2d2::Pool<PostgresConnectionManager<NoTls>>,
    ) -> eyre::Result<()> {
        crate::change_stream::initialize(&mut crate::pg::PgDb::try_from_pool(&pg_pool)?)?;
        let workers = ThreadPool::new(32);
        let cursors = Arc::new(Mutex::new(Cursors::default()));
        let cleanup = Arc::downgrade(&cursors);
        std::thread::spawn(move || {
            while let Some(cursors) = cleanup.upgrade() {
                if let Ok(mut cursors) = cursors.lock() {
                    cursors.sessions.expire();
                }
                drop(cursors);
                std::thread::sleep(Duration::from_secs(1));
            }
        });
        let journal_pool = pg_pool.clone();
        std::thread::spawn(move || {
            let mut pruned = Instant::now();
            loop {
                std::thread::sleep(Duration::from_millis(100));
                if let Ok(mut client) = crate::pg::PgDb::try_from_pool(&journal_pool) {
                    if let Err(error) = crate::change_stream::pump(&mut client) {
                        log::warn!("change journal decoding failed: {error}");
                    }
                    if pruned.elapsed() >= Duration::from_secs(60) {
                        if let Err(error) = crate::change_stream::prune(&mut client) {
                            log::warn!("change journal cleanup failed: {error}");
                        }
                        pruned = Instant::now();
                    }
                }
            }
        });
        log::info!("OxideDB listening on {}", listener.local_addr()?);
        for stream in listener.incoming() {
            let stream = match stream {
                Ok(stream) => stream,
                Err(error) => {
                    log::warn!("accept failed: {error}");
                    continue;
                }
            };
            stream.set_nodelay(true)?;
            stream.set_read_timeout(Some(Duration::from_secs(30)))?;
            stream.set_write_timeout(Some(Duration::from_secs(30)))?;
            let pool = pg_pool.clone();
            let cursors = cursors.clone();
            workers.execute(move || {
                if let Err(error) = handle_connection(stream, pool, cursors) {
                    log::debug!("connection closed: {error}");
                }
            });
        }
        Ok(())
    }
}
fn handle_connection(
    mut stream: TcpStream,
    pool: r2d2::Pool<PostgresConnectionManager<NoTls>>,
    cursors: Arc<Mutex<Cursors>>,
) -> eyre::Result<()> {
    let addr = stream.peer_addr()?;
    loop {
        // Drivers keep pooled connections idle for minutes. An idle socket is
        // not an incomplete frame: start the frame deadline at the first byte.
        stream.set_read_timeout(None)?;
        let mut length = [0_u8; 4];
        match stream.read_exact(&mut length[..1]) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        match read_before(&mut stream, &mut length[1..], deadline) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        }
        let size = i32::from_le_bytes(length);
        if size < wire::HEADER_SIZE as i32 || size > wire::MAX_MSG_LEN as i32 {
            eyre::bail!("invalid frame length");
        }
        let mut buffer = vec![0; size as usize];
        buffer[..4].copy_from_slice(&length);
        read_before(&mut stream, &mut buffer[4..], deadline)?;
        let opcode = wire::parse(&buffer)?;
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        // A final containment boundary keeps unexpected implementation panics
        // from permanently reducing serving capacity. Validation returns errors.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handle_with_cursors(id, &pool, addr, &opcode, &cursors)
        }));
        let response=match result{
            Ok(Ok(response))=>response,
            Ok(Err(error))=>opcode.reply(Response::new(id,&opcode,vec![doc!{"ok":0.0,"errmsg":error.message,"code":error.code,"codeName":error.code_name,"errorLabels":if [112,251].contains(&error.code) {vec!["TransientTransactionError"]} else {vec![]}}]))?,
            Err(_)=>{log::error!("unexpected command panic");opcode.reply(Response::new(id,&opcode,vec![doc!{"ok":0.0,"errmsg":"internal command error","code":1,"codeName":"InternalError"}]))?}
        };
        if matches!(&opcode,OpCode::OpMsg(msg)if msg.flags&wire::MORE_TO_COME!=0) {
            continue;
        }
        if response.len() > wire::MAX_MSG_LEN as usize {
            eyre::bail!("response exceeds wire limit");
        }
        let mut remaining = response.as_slice();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !remaining.is_empty() {
            let timeout = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or_else(|| eyre::eyre!("reply deadline elapsed"))?;
            stream.set_write_timeout(Some(timeout))?;
            match stream.write(remaining) {
                Ok(0) => eyre::bail!("connection closed during reply"),
                Ok(written) => remaining = &remaining[written..],
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}

// The deadline spans the entire frame, so a slow trickle cannot reset it.
fn read_before(
    stream: &mut TcpStream,
    mut buffer: &mut [u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !buffer.is_empty() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "frame deadline elapsed")
            })?;
        stream.set_read_timeout(Some(remaining))?;
        match stream.read(buffer) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => buffer = &mut buffer[n..],
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
