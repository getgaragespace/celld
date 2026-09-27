// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A SQLite VFS over the host filesystem, installed as the process default so
//! that celld's cell databases and the LTX engine's connections use it without
//! naming it.
//!
//! File contents live in the host filesystem. Locks and the WAL index live in
//! this process, because every connection to these databases is in this
//! process: the lock table follows SQLite's unix VFS semantics, and the WAL
//! index is heap memory shared by the connections to one database, which is
//! what the `-shm` mapping provides on a real filesystem.

use crate::bridge::{HostFs, OpenMode};
use rusqlite::ffi;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::io;
use std::ptr;
use std::sync::atomic::{fence, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

const VFS_NAME: &CStr = c"celld-host";
const MAX_PATHNAME: c_int = 1024;
const SECTOR_SIZE: c_int = 4096;
const SHM_LOCKS: usize = ffi::SQLITE_SHM_NLOCK as usize;

static HOST: OnceLock<Arc<HostFs>> = OnceLock::new();
static BASE: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(ptr::null_mut());
static LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<FileLock>>>>> = OnceLock::new();
static SHMS: OnceLock<Mutex<HashMap<String, Arc<Mutex<Shm>>>>> = OnceLock::new();

/// Register the VFS as SQLite's default. Must run before the node opens a
/// database.
pub fn install(host: Arc<HostFs>) -> anyhow::Result<()> {
    HOST.set(host)
        .map_err(|_| anyhow::anyhow!("the SQLite host VFS is already installed"))?;
    unsafe {
        anyhow::ensure!(
            ffi::sqlite3_initialize() == ffi::SQLITE_OK,
            "sqlite3_initialize failed"
        );
        let base = ffi::sqlite3_vfs_find(ptr::null());
        anyhow::ensure!(!base.is_null(), "SQLite has no default VFS to delegate to");
        BASE.store(base, Ordering::Release);
        let vfs = Box::leak(Box::new(ffi::sqlite3_vfs {
            iVersion: 2,
            szOsFile: std::mem::size_of::<VfsFile>() as c_int,
            mxPathname: MAX_PATHNAME,
            pNext: ptr::null_mut(),
            zName: VFS_NAME.as_ptr(),
            pAppData: ptr::null_mut(),
            xOpen: Some(x_open),
            xDelete: Some(x_delete),
            xAccess: Some(x_access),
            xFullPathname: Some(x_full_pathname),
            xDlOpen: Some(x_dl_open),
            xDlError: Some(x_dl_error),
            xDlSym: Some(x_dl_sym),
            xDlClose: Some(x_dl_close),
            xRandomness: Some(x_randomness),
            xSleep: Some(x_sleep),
            xCurrentTime: Some(x_current_time),
            xGetLastError: Some(x_get_last_error),
            xCurrentTimeInt64: Some(x_current_time_int64),
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        }));
        let rc = ffi::sqlite3_vfs_register(vfs, 1);
        anyhow::ensure!(rc == ffi::SQLITE_OK, "sqlite3_vfs_register failed: {rc}");
    }
    Ok(())
}

fn host() -> &'static HostFs {
    HOST.get().expect("the SQLite host VFS is installed")
}

fn base() -> *mut ffi::sqlite3_vfs {
    BASE.load(Ordering::Acquire)
}

#[repr(C)]
struct VfsFile {
    base: ffi::sqlite3_file,
    state: *mut FileState,
}

enum Backing {
    Host {
        fd: i64,
    },
    /// A temporary file SQLite opened without a name.
    Memory(Vec<u8>),
}

struct FileState {
    path: Option<String>,
    backing: Backing,
    lock: Option<LockHold>,
    shm: Option<ShmHold>,
}

#[derive(Default)]
struct FileLock {
    shared: u32,
    reserved: bool,
    pending: bool,
    exclusive: bool,
}

/// One connection's hold on a file's lock.
struct LockHold {
    path: String,
    lock: Arc<Mutex<FileLock>>,
    level: c_int,
    reserved: bool,
    pending: bool,
    exclusive: bool,
}

impl LockHold {
    fn acquire(path: &str) -> Self {
        let lock = LOCKS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .entry(path.to_string())
            .or_default()
            .clone();
        Self {
            path: path.to_string(),
            lock,
            level: ffi::SQLITE_LOCK_NONE,
            reserved: false,
            pending: false,
            exclusive: false,
        }
    }

    fn lock(&mut self, level: c_int) -> c_int {
        if self.level >= level {
            return ffi::SQLITE_OK;
        }
        let mut shared = self.lock.lock().unwrap();
        match level {
            ffi::SQLITE_LOCK_SHARED => {
                if shared.pending || shared.exclusive {
                    return ffi::SQLITE_BUSY;
                }
                shared.shared += 1;
            }
            ffi::SQLITE_LOCK_RESERVED => {
                if shared.reserved {
                    return ffi::SQLITE_BUSY;
                }
                shared.reserved = true;
                self.reserved = true;
            }
            ffi::SQLITE_LOCK_EXCLUSIVE => {
                if shared.reserved && !self.reserved {
                    return ffi::SQLITE_BUSY;
                }
                if !self.pending {
                    if shared.pending {
                        return ffi::SQLITE_BUSY;
                    }
                    shared.pending = true;
                    self.pending = true;
                }
                if shared.shared > 1 {
                    self.level = ffi::SQLITE_LOCK_PENDING;
                    return ffi::SQLITE_BUSY;
                }
                shared.exclusive = true;
                self.exclusive = true;
            }
            _ => return ffi::SQLITE_MISUSE,
        }
        self.level = level;
        ffi::SQLITE_OK
    }

    fn unlock(&mut self, level: c_int) -> c_int {
        if self.level <= level {
            return ffi::SQLITE_OK;
        }
        let mut shared = self.lock.lock().unwrap();
        if std::mem::take(&mut self.exclusive) {
            shared.exclusive = false;
        }
        if std::mem::take(&mut self.pending) {
            shared.pending = false;
        }
        if std::mem::take(&mut self.reserved) {
            shared.reserved = false;
        }
        if level == ffi::SQLITE_LOCK_NONE && self.level >= ffi::SQLITE_LOCK_SHARED {
            shared.shared -= 1;
        }
        self.level = level;
        ffi::SQLITE_OK
    }

    fn reserved_by_anyone(&self) -> bool {
        let shared = self.lock.lock().unwrap();
        shared.reserved || shared.pending || shared.exclusive
    }
}

impl Drop for LockHold {
    fn drop(&mut self) {
        self.unlock(ffi::SQLITE_LOCK_NONE);
        let mut locks = LOCKS.get_or_init(Default::default).lock().unwrap();
        if locks
            .get(&self.path)
            .is_some_and(|lock| Arc::ptr_eq(lock, &self.lock) && Arc::strong_count(lock) == 2)
        {
            locks.remove(&self.path);
        }
    }
}

#[derive(Default)]
struct Shm {
    regions: Vec<Box<[u8]>>,
    shared: [u32; SHM_LOCKS],
    exclusive: [bool; SHM_LOCKS],
}

/// One connection's mapping of a database's WAL index.
struct ShmHold {
    path: String,
    shm: Arc<Mutex<Shm>>,
    shared_mask: u16,
    exclusive_mask: u16,
}

impl ShmHold {
    fn open(path: &str) -> Self {
        let shm = SHMS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .entry(path.to_string())
            .or_default()
            .clone();
        Self {
            path: path.to_string(),
            shm,
            shared_mask: 0,
            exclusive_mask: 0,
        }
    }

    fn map(&mut self, region: usize, size: usize, extend: bool) -> *mut c_void {
        let mut shm = self.shm.lock().unwrap();
        while shm.regions.len() <= region {
            if !extend {
                return ptr::null_mut();
            }
            shm.regions.push(vec![0u8; size].into_boxed_slice());
        }
        shm.regions[region].as_mut_ptr().cast()
    }

    fn lock(&mut self, offset: usize, count: usize, flags: c_int) -> c_int {
        let mut shm = self.shm.lock().unwrap();
        let slots = offset..offset + count;
        let bit = |slot: usize| 1u16 << slot;
        if flags & ffi::SQLITE_SHM_UNLOCK != 0 {
            for slot in slots {
                if self.exclusive_mask & bit(slot) != 0 {
                    shm.exclusive[slot] = false;
                    self.exclusive_mask &= !bit(slot);
                }
                if self.shared_mask & bit(slot) != 0 {
                    shm.shared[slot] -= 1;
                    self.shared_mask &= !bit(slot);
                }
            }
            return ffi::SQLITE_OK;
        }
        if flags & ffi::SQLITE_SHM_SHARED != 0 {
            for slot in slots.clone() {
                if shm.exclusive[slot] && self.exclusive_mask & bit(slot) == 0 {
                    return ffi::SQLITE_BUSY;
                }
            }
            for slot in slots {
                if self.shared_mask & bit(slot) == 0 {
                    shm.shared[slot] += 1;
                    self.shared_mask |= bit(slot);
                }
            }
            return ffi::SQLITE_OK;
        }
        for slot in slots.clone() {
            let mine = u32::from(self.shared_mask & bit(slot) != 0);
            if (shm.exclusive[slot] && self.exclusive_mask & bit(slot) == 0)
                || shm.shared[slot] > mine
            {
                return ffi::SQLITE_BUSY;
            }
        }
        for slot in slots {
            shm.exclusive[slot] = true;
            self.exclusive_mask |= bit(slot);
        }
        ffi::SQLITE_OK
    }
}

impl Drop for ShmHold {
    fn drop(&mut self) {
        self.lock(0, SHM_LOCKS, ffi::SQLITE_SHM_UNLOCK);
        let mut shms = SHMS.get_or_init(Default::default).lock().unwrap();
        if shms
            .get(&self.path)
            .is_some_and(|shm| Arc::ptr_eq(shm, &self.shm) && Arc::strong_count(shm) == 2)
        {
            shms.remove(&self.path);
        }
    }
}

static IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

unsafe fn state<'a>(file: *mut ffi::sqlite3_file) -> &'a mut FileState {
    &mut *(*file.cast::<VfsFile>()).state
}

unsafe fn path_arg(name: *const c_char) -> Option<String> {
    if name.is_null() {
        return None;
    }
    CStr::from_ptr(name).to_str().ok().map(str::to_string)
}

unsafe extern "C" fn x_open(
    _vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let slot = file.cast::<VfsFile>();
    (*slot).base.pMethods = ptr::null();
    (*slot).state = ptr::null_mut();
    let path = path_arg(name);
    let backing = match &path {
        None => Backing::Memory(Vec::new()),
        Some(path) => {
            if flags & ffi::SQLITE_OPEN_EXCLUSIVE != 0 && host().stat(path).is_ok() {
                return ffi::SQLITE_CANTOPEN;
            }
            let mode = if flags & ffi::SQLITE_OPEN_CREATE != 0 {
                OpenMode::Create
            } else {
                OpenMode::Existing
            };
            match host().open(path, mode) {
                Ok(fd) => Backing::Host { fd },
                Err(_) => return ffi::SQLITE_CANTOPEN,
            }
        }
    };
    // As the unix VFS does: a delete-on-close file loses its name at once and
    // lives on through the open descriptor.
    if let (Some(path), true) = (&path, flags & ffi::SQLITE_OPEN_DELETEONCLOSE != 0) {
        let _ = host().unlink(path);
    }
    let lock = path.as_deref().map(LockHold::acquire);
    (*slot).state = Box::into_raw(Box::new(FileState {
        path,
        backing,
        lock,
        shm: None,
    }));
    (*slot).base.pMethods = &IO_METHODS;
    if !out_flags.is_null() {
        *out_flags = flags;
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    let slot = file.cast::<VfsFile>();
    if (*slot).state.is_null() {
        return ffi::SQLITE_OK;
    }
    let state = Box::from_raw((*slot).state);
    (*slot).state = ptr::null_mut();
    if let Backing::Host { fd } = state.backing {
        host().release(fd);
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buffer: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let out = std::slice::from_raw_parts_mut(buffer.cast::<u8>(), amount as usize);
    let offset = offset as u64;
    let read = match &state(file).backing {
        Backing::Memory(data) => {
            let start = (offset as usize).min(data.len());
            let end = (start + out.len()).min(data.len());
            out[..end - start].copy_from_slice(&data[start..end]);
            end - start
        }
        Backing::Host { fd } => match host().read_at(*fd, offset, out.len()) {
            Ok(bytes) => {
                out[..bytes.len()].copy_from_slice(&bytes);
                bytes.len()
            }
            Err(_) => return ffi::SQLITE_IOERR_READ,
        },
    };
    if read < out.len() {
        out[read..].fill(0);
        return ffi::SQLITE_IOERR_SHORT_READ;
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buffer: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let data = std::slice::from_raw_parts(buffer.cast::<u8>(), amount as usize);
    let offset = offset as usize;
    match &mut state(file).backing {
        Backing::Memory(content) => {
            if content.len() < offset + data.len() {
                content.resize(offset + data.len(), 0);
            }
            content[offset..offset + data.len()].copy_from_slice(data);
            ffi::SQLITE_OK
        }
        Backing::Host { fd } => match host().write_at(*fd, offset as u64, data) {
            Ok(()) => ffi::SQLITE_OK,
            Err(_) => ffi::SQLITE_IOERR_WRITE,
        },
    }
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    match &mut state(file).backing {
        Backing::Memory(content) => {
            content.truncate(size as usize);
            ffi::SQLITE_OK
        }
        Backing::Host { fd } => match host().truncate(*fd, size as u64) {
            Ok(()) => ffi::SQLITE_OK,
            Err(_) => ffi::SQLITE_IOERR_TRUNCATE,
        },
    }
}

unsafe extern "C" fn x_sync(_file: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_size(
    file: *mut ffi::sqlite3_file,
    size: *mut ffi::sqlite3_int64,
) -> c_int {
    match &state(file).backing {
        Backing::Memory(content) => {
            *size = content.len() as ffi::sqlite3_int64;
            ffi::SQLITE_OK
        }
        Backing::Host { fd } => match host().fd_stat(*fd) {
            Ok(stat) => {
                *size = stat.size as ffi::sqlite3_int64;
                ffi::SQLITE_OK
            }
            Err(_) => ffi::SQLITE_IOERR_FSTAT,
        },
    }
}

unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    match &mut state(file).lock {
        Some(hold) => hold.lock(level),
        None => ffi::SQLITE_OK,
    }
}

unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    match &mut state(file).lock {
        Some(hold) => hold.unlock(level),
        None => ffi::SQLITE_OK,
    }
}

unsafe extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    *out = match &state(file).lock {
        Some(hold) => c_int::from(hold.reserved_by_anyone()),
        None => 0,
    };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_file_control(
    _file: *mut ffi::sqlite3_file,
    _op: c_int,
    _arg: *mut c_void,
) -> c_int {
    ffi::SQLITE_NOTFOUND
}

unsafe extern "C" fn x_sector_size(_file: *mut ffi::sqlite3_file) -> c_int {
    SECTOR_SIZE
}

unsafe extern "C" fn x_device_characteristics(_file: *mut ffi::sqlite3_file) -> c_int {
    ffi::SQLITE_IOCAP_POWERSAFE_OVERWRITE
}

unsafe extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    region: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    let state = state(file);
    let Some(path) = state.path.clone() else {
        return ffi::SQLITE_IOERR_SHMOPEN;
    };
    let hold = state.shm.get_or_insert_with(|| ShmHold::open(&path));
    *out = hold.map(region as usize, size as usize, extend != 0);
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    count: c_int,
    flags: c_int,
) -> c_int {
    match &mut state(file).shm {
        Some(hold) => hold.lock(offset as usize, count as usize, flags),
        None => ffi::SQLITE_IOERR_SHMLOCK,
    }
}

unsafe extern "C" fn x_shm_barrier(_file: *mut ffi::sqlite3_file) {
    fence(Ordering::SeqCst);
}

unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, _delete: c_int) -> c_int {
    state(file).shm = None;
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_delete(
    _vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    _sync_dir: c_int,
) -> c_int {
    let Some(path) = path_arg(name) else {
        return ffi::SQLITE_IOERR_DELETE;
    };
    match host().unlink(&path) {
        Ok(()) => ffi::SQLITE_OK,
        Err(error) if error.kind() == io::ErrorKind::NotFound => ffi::SQLITE_IOERR_DELETE_NOENT,
        Err(_) => ffi::SQLITE_IOERR_DELETE,
    }
}

unsafe extern "C" fn x_access(
    _vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    let Some(path) = path_arg(name) else {
        *out = 0;
        return ffi::SQLITE_OK;
    };
    *out = match host().stat(&path) {
        // The unix VFS reports an empty file as absent, so SQLite does not
        // mistake an empty journal for a hot one.
        Ok(stat) if flags == ffi::SQLITE_ACCESS_EXISTS => c_int::from(stat.is_dir || stat.size > 0),
        Ok(_) => 1,
        Err(_) => 0,
    };
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_full_pathname(
    _vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    size: c_int,
    out: *mut c_char,
) -> c_int {
    let Some(path) = path_arg(name) else {
        return ffi::SQLITE_CANTOPEN;
    };
    let full = if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    };
    if full.len() + 1 > size as usize {
        return ffi::SQLITE_CANTOPEN;
    }
    ptr::copy_nonoverlapping(full.as_ptr().cast::<c_char>(), out, full.len());
    *out.add(full.len()) = 0;
    ffi::SQLITE_OK
}

unsafe extern "C" fn x_dl_open(_vfs: *mut ffi::sqlite3_vfs, name: *const c_char) -> *mut c_void {
    (*base())
        .xDlOpen
        .map_or(ptr::null_mut(), |f| f(base(), name))
}

unsafe extern "C" fn x_dl_error(_vfs: *mut ffi::sqlite3_vfs, size: c_int, message: *mut c_char) {
    if let Some(f) = (*base()).xDlError {
        f(base(), size, message);
    }
}

unsafe extern "C" fn x_dl_sym(
    _vfs: *mut ffi::sqlite3_vfs,
    handle: *mut c_void,
    symbol: *const c_char,
) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
    (*base()).xDlSym.and_then(|f| f(base(), handle, symbol))
}

unsafe extern "C" fn x_dl_close(_vfs: *mut ffi::sqlite3_vfs, handle: *mut c_void) {
    if let Some(f) = (*base()).xDlClose {
        f(base(), handle);
    }
}

unsafe extern "C" fn x_randomness(
    _vfs: *mut ffi::sqlite3_vfs,
    size: c_int,
    out: *mut c_char,
) -> c_int {
    (*base()).xRandomness.map_or(0, |f| f(base(), size, out))
}

unsafe extern "C" fn x_sleep(_vfs: *mut ffi::sqlite3_vfs, microseconds: c_int) -> c_int {
    (*base()).xSleep.map_or(0, |f| f(base(), microseconds))
}

unsafe extern "C" fn x_current_time(_vfs: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    (*base())
        .xCurrentTime
        .map_or(ffi::SQLITE_ERROR, |f| f(base(), out))
}

unsafe extern "C" fn x_get_last_error(
    _vfs: *mut ffi::sqlite3_vfs,
    size: c_int,
    out: *mut c_char,
) -> c_int {
    (*base()).xGetLastError.map_or(0, |f| f(base(), size, out))
}

unsafe extern "C" fn x_current_time_int64(
    _vfs: *mut ffi::sqlite3_vfs,
    out: *mut ffi::sqlite3_int64,
) -> c_int {
    (*base())
        .xCurrentTimeInt64
        .map_or(ffi::SQLITE_ERROR, |f| f(base(), out))
}
