// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! celld's injected filesystem, served by the host filesystem.

use crate::bridge::{HostFs, OpenMode};
use celld_ltx::{FileSystem, HostDirEntry, HostFile, HostFileIo, HostMetadata};
use std::io;
use std::path::Path;
use std::sync::Arc;

pub struct NodeFileSystem {
    host: Arc<HostFs>,
}

impl NodeFileSystem {
    pub fn new(host: Arc<HostFs>) -> Self {
        Self { host }
    }
}

pub(crate) fn host_path(path: &Path) -> io::Result<&str> {
    let text = path
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path is not UTF-8"))?;
    if !text.starts_with('/') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("host filesystem paths are absolute: {text}"),
        ));
    }
    Ok(text)
}

struct NodeFile {
    host: Arc<HostFs>,
    fd: i64,
    position: u64,
}

impl HostFileIo for NodeFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.host.write_at(self.fd, self.position, bytes)?;
        self.position += bytes.len() as u64;
        Ok(())
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let bytes = self.host.read_at(self.fd, offset, len)?;
        if bytes.len() < len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        Ok(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn file_len(&mut self) -> io::Result<u64> {
        Ok(self.host.fd_stat(self.fd)?.size)
    }
}

impl Drop for NodeFile {
    fn drop(&mut self) {
        self.host.release(self.fd);
    }
}

/// An anonymous scratch file. It has no name, so it needs no host storage:
/// the buffer lives and dies with its handle.
#[derive(Default)]
struct ScratchFile {
    data: Vec<u8>,
}

impl HostFileIo for ScratchFile {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.data.extend_from_slice(bytes);
        Ok(())
    }

    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        self.data
            .get(start..start.saturating_add(len))
            .map(<[u8]>::to_vec)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer"))
    }

    fn sync_all(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn file_len(&mut self) -> io::Result<u64> {
        Ok(self.data.len() as u64)
    }
}

impl FileSystem for NodeFileSystem {
    fn temporary_file(&self, _directory: Option<&Path>) -> io::Result<HostFile> {
        Ok(HostFile::from_io(ScratchFile::default()))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.host.read_file(host_path(path)?)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        Ok(self
            .host
            .read_dir(host_path(path)?)?
            .into_iter()
            .map(|entry| HostDirEntry {
                path: path.join(&entry.name),
                file_name: entry.name.into(),
                is_dir: entry.is_dir,
            })
            .collect())
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        let stat = self.host.stat(host_path(path)?)?;
        Ok(HostMetadata {
            len: stat.size,
            is_dir: stat.is_dir,
            is_file: stat.is_file,
            modified_unix_millis: stat.mtime_ms,
        })
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        let fd = self.host.open(host_path(path)?, OpenMode::Truncate)?;
        Ok(HostFile::from_io(NodeFile {
            host: self.host.clone(),
            fd,
            position: 0,
        }))
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        let fd = self.host.open(host_path(path)?, OpenMode::Existing)?;
        Ok(HostFile::from_io(NodeFile {
            host: self.host.clone(),
            fd,
            position: 0,
        }))
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.host.write_file(host_path(path)?, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.host.stat(host_path(path)?).map(drop)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.host.rename(host_path(from)?, host_path(to)?)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.host.unlink(host_path(path)?)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.host.remove_all(host_path(path)?)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.host.mkdir_all(host_path(path)?)
    }
}
