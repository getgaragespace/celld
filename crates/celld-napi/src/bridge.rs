// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The host filesystem: a JavaScript virtual filesystem (`node:vfs`) reached
//! from the node's threads.
//!
//! Every call crosses to the JavaScript thread through a thread-safe function
//! and blocks until the host answers, so the JavaScript thread must never wait
//! on the node itself. The addon's API is asynchronous for that reason.
//!
//! `node:vfs` (v26.10) keeps one content buffer per open descriptor, so two
//! descriptors on one file do not observe each other's writes. SQLite, the LTX
//! replicator and celld open the same files at the same time, so [`HostFs`]
//! holds exactly one host descriptor per path and shares it among every
//! opener. A rename moves that descriptor to the new path and an unlink
//! detaches it, which matches what `node:vfs` does with the open handle.

use napi::threadsafe_function::{
    ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{JsBuffer, JsFunction, JsObject, JsUnknown, ValueType};
use std::collections::HashMap;
use std::io;
use std::sync::{mpsc, Mutex};
use std::thread::ThreadId;
use std::time::Duration;

/// How long a node thread waits for the JavaScript thread before it reports
/// the host as unresponsive instead of hanging.
const HOST_REPLY_TIMEOUT: Duration = Duration::from_secs(30);

pub enum Op {
    Open {
        path: String,
        flags: &'static str,
    },
    Close {
        fd: i64,
    },
    Read {
        fd: i64,
        position: u64,
        length: usize,
    },
    Write {
        fd: i64,
        position: u64,
        data: Vec<u8>,
    },
    Truncate {
        fd: i64,
        length: u64,
    },
    Fstat {
        fd: i64,
    },
    Stat {
        path: String,
    },
    ReadFile {
        path: String,
    },
    WriteFile {
        path: String,
        data: Vec<u8>,
    },
    ReadDir {
        path: String,
    },
    MkdirAll {
        path: String,
    },
    Rename {
        from: String,
        to: String,
    },
    Unlink {
        path: String,
    },
    RemoveAll {
        path: String,
    },
}

#[derive(Clone, Copy)]
enum Shape {
    Unit,
    Number,
    Bytes,
    Stat,
    Entries,
}

impl Op {
    fn name(&self) -> &'static str {
        match self {
            Op::Open { .. } => "open",
            Op::Close { .. } => "close",
            Op::Read { .. } => "read",
            Op::Write { .. } => "write",
            Op::Truncate { .. } => "truncate",
            Op::Fstat { .. } => "fstat",
            Op::Stat { .. } => "stat",
            Op::ReadFile { .. } => "readFile",
            Op::WriteFile { .. } => "writeFile",
            Op::ReadDir { .. } => "readdir",
            Op::MkdirAll { .. } => "mkdirAll",
            Op::Rename { .. } => "rename",
            Op::Unlink { .. } => "unlink",
            Op::RemoveAll { .. } => "rmAll",
        }
    }

    fn shape(&self) -> Shape {
        match self {
            Op::Open { .. } | Op::Write { .. } => Shape::Number,
            Op::Read { .. } | Op::ReadFile { .. } => Shape::Bytes,
            Op::Fstat { .. } | Op::Stat { .. } => Shape::Stat,
            Op::ReadDir { .. } => Shape::Entries,
            _ => Shape::Unit,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Stat {
    pub size: u64,
    pub is_dir: bool,
    pub is_file: bool,
    pub mtime_ms: i64,
}

pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

enum Reply {
    Unit,
    Number(f64),
    Bytes(Vec<u8>),
    Stat(Stat),
    Entries(Vec<DirEntry>),
}

type Answer = mpsc::Sender<io::Result<Reply>>;

/// The raw thread-safe call into the JavaScript dispatcher.
struct Dispatcher {
    function: ThreadsafeFunction<Op, ErrorStrategy::Fatal>,
    js_thread: ThreadId,
}

impl Dispatcher {
    fn call(&self, op: Op) -> io::Result<Reply> {
        if std::thread::current().id() == self.js_thread {
            return Err(io::Error::other(
                "the host filesystem was called on the JavaScript thread, which would deadlock",
            ));
        }
        let name = op.name();
        let shape = op.shape();
        let (answer, receive): (Answer, _) = mpsc::channel();
        let status = self.function.call_with_return_value(
            op,
            ThreadsafeFunctionCallMode::Blocking,
            move |value: JsUnknown| {
                let _ = answer.send(parse_reply(value, shape));
                Ok(())
            },
        );
        if status != napi::Status::Ok {
            return Err(io::Error::other(format!(
                "host filesystem {name} could not be queued: {status:?}"
            )));
        }
        receive.recv_timeout(HOST_REPLY_TIMEOUT).map_err(|_| {
            io::Error::other(format!(
                "host filesystem {name} got no answer in {}s; is the JavaScript thread blocked?",
                HOST_REPLY_TIMEOUT.as_secs()
            ))
        })?
    }
}

fn to_js_args(context: ThreadSafeCallContext<Op>) -> napi::Result<Vec<JsUnknown>> {
    let env = context.env;
    let string = |value: &str| env.create_string(value).map(|s| s.into_unknown());
    let number = |value: f64| env.create_double(value).map(|n| n.into_unknown());
    let buffer = |value: Vec<u8>| env.create_buffer_with_data(value).map(|b| b.into_unknown());
    let op = context.value;
    let mut arguments = vec![string(op.name())?];
    match op {
        Op::Open { path, flags } => arguments.extend([string(&path)?, string(flags)?]),
        Op::Close { fd } | Op::Fstat { fd } => arguments.push(number(fd as f64)?),
        Op::Read {
            fd,
            position,
            length,
        } => arguments.extend([
            number(fd as f64)?,
            number(position as f64)?,
            number(length as f64)?,
        ]),
        Op::Write { fd, position, data } => {
            arguments.extend([number(fd as f64)?, number(position as f64)?, buffer(data)?])
        }
        Op::Truncate { fd, length } => {
            arguments.extend([number(fd as f64)?, number(length as f64)?])
        }
        Op::Stat { path }
        | Op::ReadFile { path }
        | Op::ReadDir { path }
        | Op::MkdirAll { path }
        | Op::Unlink { path }
        | Op::RemoveAll { path } => arguments.push(string(&path)?),
        Op::WriteFile { path, data } => arguments.extend([string(&path)?, buffer(data)?]),
        Op::Rename { from, to } => arguments.extend([string(&from)?, string(&to)?]),
    }
    Ok(arguments)
}

/// The dispatcher answers `[null, value]` or `[code, message]`.
fn parse_reply(value: JsUnknown, shape: Shape) -> io::Result<Reply> {
    let js = |error: napi::Error| io::Error::other(format!("host filesystem reply: {error}"));
    let pair: JsObject = value.coerce_to_object().map_err(js)?;
    let code: JsUnknown = pair.get_element(0).map_err(js)?;
    if code.get_type().map_err(js)? != ValueType::Null {
        let code = code
            .coerce_to_string()
            .and_then(|s| s.into_utf8())
            .and_then(|s| s.into_owned())
            .map_err(js)?;
        let message: JsUnknown = pair.get_element(1).map_err(js)?;
        let message = message
            .coerce_to_string()
            .and_then(|s| s.into_utf8())
            .and_then(|s| s.into_owned())
            .unwrap_or_default();
        return Err(host_error(&code, message));
    }
    let value: JsUnknown = pair.get_element(1).map_err(js)?;
    Ok(match shape {
        Shape::Unit => Reply::Unit,
        Shape::Number => Reply::Number(
            value
                .coerce_to_number()
                .and_then(|n| n.get_double())
                .map_err(js)?,
        ),
        Shape::Bytes => {
            if !value.is_buffer().map_err(js)? {
                return Err(io::Error::other("host filesystem reply is not a Buffer"));
            }
            let buffer: JsBuffer = unsafe { value.cast() };
            Reply::Bytes(buffer.into_value().map_err(js)?.to_vec())
        }
        Shape::Stat => {
            let stat: JsObject = value.coerce_to_object().map_err(js)?;
            let number = |key: &str| -> io::Result<f64> {
                stat.get_named_property::<JsUnknown>(key)
                    .and_then(|v| v.coerce_to_number())
                    .and_then(|n| n.get_double())
                    .map_err(js)
            };
            let flag = |key: &str| -> io::Result<bool> {
                stat.get_named_property::<JsUnknown>(key)
                    .and_then(|v| v.coerce_to_bool())
                    .and_then(|b| b.get_value())
                    .map_err(js)
            };
            Reply::Stat(Stat {
                size: number("size")? as u64,
                is_dir: flag("dir")?,
                is_file: flag("file")?,
                mtime_ms: number("mtimeMs")? as i64,
            })
        }
        Shape::Entries => {
            let list: JsObject = value.coerce_to_object().map_err(js)?;
            let length = list.get_array_length().map_err(js)?;
            let mut entries = Vec::with_capacity(length as usize);
            for index in 0..length {
                let name: JsUnknown = list.get_element(index).map_err(js)?;
                let name = name
                    .coerce_to_string()
                    .and_then(|s| s.into_utf8())
                    .and_then(|s| s.into_owned())
                    .map_err(js)?;
                entries.push(match name.strip_suffix('/') {
                    Some(directory) => DirEntry {
                        name: directory.to_string(),
                        is_dir: true,
                    },
                    None => DirEntry {
                        name,
                        is_dir: false,
                    },
                });
            }
            Reply::Entries(entries)
        }
    })
}

fn host_error(code: &str, message: String) -> io::Error {
    let errno = match code {
        "ENOENT" => libc::ENOENT,
        "EEXIST" => libc::EEXIST,
        "EISDIR" => libc::EISDIR,
        "ENOTDIR" => libc::ENOTDIR,
        "ENOTEMPTY" => libc::ENOTEMPTY,
        "EACCES" => libc::EACCES,
        "EPERM" => libc::EPERM,
        "EBADF" => libc::EBADF,
        "EINVAL" => libc::EINVAL,
        "EROFS" => libc::EROFS,
        "ELOOP" => libc::ELOOP,
        _ => return io::Error::other(format!("{code}: {message}")),
    };
    let kind = io::Error::from_raw_os_error(errno).kind();
    io::Error::new(kind, format!("{code}: {message}"))
}

#[derive(Default)]
struct Table {
    by_path: HashMap<String, i64>,
    references: HashMap<i64, usize>,
}

/// The host filesystem with one shared descriptor per open path.
pub struct HostFs {
    dispatcher: Dispatcher,
    table: Mutex<Table>,
}

/// How [`HostFs::open`] treats a missing or existing file.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// The file must exist.
    Existing,
    /// Create the file when it is missing; keep its contents otherwise.
    Create,
    /// Create the file when it is missing and empty it otherwise.
    Truncate,
}

impl HostFs {
    pub fn new(env: &napi::Env, dispatch: JsFunction) -> napi::Result<Self> {
        let mut function: ThreadsafeFunction<Op, ErrorStrategy::Fatal> =
            dispatch.create_threadsafe_function(0, to_js_args)?;
        // The host may exit while the node is idle; the node holds no claim on
        // the host's event loop.
        function.unref(env)?;
        Ok(Self {
            dispatcher: Dispatcher {
                function,
                js_thread: std::thread::current().id(),
            },
            table: Mutex::new(Table::default()),
        })
    }

    fn call(&self, op: Op) -> io::Result<Reply> {
        self.dispatcher.call(op)
    }

    /// Open `path` read-write, sharing the descriptor with every other opener.
    pub fn open(&self, path: &str, mode: OpenMode) -> io::Result<i64> {
        let mut table = self.table.lock().unwrap();
        if let Some(&fd) = table.by_path.get(path) {
            if mode == OpenMode::Truncate {
                self.call(Op::Truncate { fd, length: 0 })?;
            }
            *table.references.entry(fd).or_default() += 1;
            return Ok(fd);
        }
        let opened = match self.call(Op::Open {
            path: path.to_string(),
            flags: "r+",
        }) {
            Ok(Reply::Number(fd)) => {
                let fd = fd as i64;
                if mode == OpenMode::Truncate {
                    self.call(Op::Truncate { fd, length: 0 })?;
                }
                fd
            }
            Ok(_) => return Err(io::Error::other("host open returned no descriptor")),
            Err(error) if error.kind() == io::ErrorKind::NotFound && mode != OpenMode::Existing => {
                match self.call(Op::Open {
                    path: path.to_string(),
                    flags: "w+",
                })? {
                    Reply::Number(fd) => fd as i64,
                    _ => return Err(io::Error::other("host open returned no descriptor")),
                }
            }
            Err(error) => return Err(error),
        };
        table.by_path.insert(path.to_string(), opened);
        table.references.insert(opened, 1);
        Ok(opened)
    }

    pub fn release(&self, fd: i64) {
        let mut table = self.table.lock().unwrap();
        let Some(count) = table.references.get_mut(&fd) else {
            return;
        };
        *count -= 1;
        if *count > 0 {
            return;
        }
        table.references.remove(&fd);
        table.by_path.retain(|_, open| *open != fd);
        let _ = self.call(Op::Close { fd });
    }

    pub fn read_at(&self, fd: i64, position: u64, length: usize) -> io::Result<Vec<u8>> {
        match self.call(Op::Read {
            fd,
            position,
            length,
        })? {
            Reply::Bytes(bytes) => Ok(bytes),
            _ => Err(io::Error::other("host read returned no bytes")),
        }
    }

    pub fn write_at(&self, fd: i64, position: u64, data: &[u8]) -> io::Result<()> {
        match self.call(Op::Write {
            fd,
            position,
            data: data.to_vec(),
        })? {
            Reply::Number(written) if written as usize == data.len() => Ok(()),
            _ => Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "host write was short",
            )),
        }
    }

    pub fn truncate(&self, fd: i64, length: u64) -> io::Result<()> {
        self.call(Op::Truncate { fd, length }).map(drop)
    }

    pub fn fd_stat(&self, fd: i64) -> io::Result<Stat> {
        match self.call(Op::Fstat { fd })? {
            Reply::Stat(stat) => Ok(stat),
            _ => Err(io::Error::other("host fstat returned no stat")),
        }
    }

    fn open_fd(&self, path: &str) -> Option<i64> {
        self.table.lock().unwrap().by_path.get(path).copied()
    }

    pub fn stat(&self, path: &str) -> io::Result<Stat> {
        if let Some(fd) = self.open_fd(path) {
            return self.fd_stat(fd);
        }
        match self.call(Op::Stat {
            path: path.to_string(),
        })? {
            Reply::Stat(stat) => Ok(stat),
            _ => Err(io::Error::other("host stat returned no stat")),
        }
    }

    pub fn read_file(&self, path: &str) -> io::Result<Vec<u8>> {
        if let Some(fd) = self.open_fd(path) {
            let size = self.fd_stat(fd)?.size as usize;
            return self.read_at(fd, 0, size);
        }
        match self.call(Op::ReadFile {
            path: path.to_string(),
        })? {
            Reply::Bytes(bytes) => Ok(bytes),
            _ => Err(io::Error::other("host readFile returned no bytes")),
        }
    }

    pub fn write_file(&self, path: &str, data: &[u8]) -> io::Result<()> {
        if let Some(fd) = self.open_fd(path) {
            self.truncate(fd, 0)?;
            return self.write_at(fd, 0, data);
        }
        self.call(Op::WriteFile {
            path: path.to_string(),
            data: data.to_vec(),
        })
        .map(drop)
    }

    pub fn read_dir(&self, path: &str) -> io::Result<Vec<DirEntry>> {
        match self.call(Op::ReadDir {
            path: path.to_string(),
        })? {
            Reply::Entries(entries) => Ok(entries),
            _ => Err(io::Error::other("host readdir returned no entries")),
        }
    }

    pub fn mkdir_all(&self, path: &str) -> io::Result<()> {
        self.call(Op::MkdirAll {
            path: path.to_string(),
        })
        .map(drop)
    }

    pub fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut table = self.table.lock().unwrap();
        self.call(Op::Rename {
            from: from.to_string(),
            to: to.to_string(),
        })?;
        table.by_path.remove(to);
        if let Some(fd) = table.by_path.remove(from) {
            table.by_path.insert(to.to_string(), fd);
        }
        Ok(())
    }

    pub fn unlink(&self, path: &str) -> io::Result<()> {
        let mut table = self.table.lock().unwrap();
        self.call(Op::Unlink {
            path: path.to_string(),
        })?;
        table.by_path.remove(path);
        Ok(())
    }

    pub fn remove_all(&self, path: &str) -> io::Result<()> {
        let mut table = self.table.lock().unwrap();
        self.call(Op::RemoveAll {
            path: path.to_string(),
        })?;
        let inside = format!("{}/", path.trim_end_matches('/'));
        table
            .by_path
            .retain(|open, _| open != path && !open.starts_with(&inside));
        Ok(())
    }
}
