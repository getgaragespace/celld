// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The fleet bucket as files in the host filesystem.
//!
//! Each object is a file under the bucket root, with a sidecar that holds its
//! etag and user metadata. One lock serializes every operation, so a
//! conditional write reads and replaces an object atomically; the node is the
//! only writer, so the lock is the whole fence.

use crate::bridge::HostFs;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::stream::{self, BoxStream, StreamExt};
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, Error, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result, UploadPart,
};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::ops::Range;
use std::sync::{Arc, Mutex};

const STORE: &str = "HostObjectStore";
const META_SUFFIX: &str = ".celld-meta.json";

#[derive(Clone)]
pub struct HostObjectStore {
    inner: Arc<Inner>,
}

struct Inner {
    host: Arc<HostFs>,
    root: String,
    lock: Mutex<()>,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Sidecar {
    e_tag: String,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

struct Stored {
    meta: ObjectMeta,
    metadata: BTreeMap<String, String>,
}

fn generic(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(error),
    }
}

fn not_found(location: &Path, error: io::Error) -> Error {
    Error::NotFound {
        path: location.to_string(),
        source: Box::new(error),
    }
}

fn new_e_tag() -> String {
    let mut bytes = [0u8; 16];
    rand::RngCore::fill_bytes(&mut celld::asyncrt::rng("host_object_store_etag"), &mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn timestamp(millis: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(millis)
        .single()
        .unwrap_or_default()
}

fn metadata_of(attributes: &Attributes) -> BTreeMap<String, String> {
    attributes
        .iter()
        .filter_map(|(key, value)| match key {
            Attribute::Metadata(name) => Some((name.to_string(), value.as_ref().to_string())),
            _ => None,
        })
        .collect()
}

fn attributes_of(metadata: &BTreeMap<String, String>) -> Attributes {
    let mut attributes = Attributes::new();
    for (name, value) in metadata {
        attributes.insert(
            Attribute::Metadata(name.clone().into()),
            value.clone().into(),
        );
    }
    attributes
}

fn resolve_range(range: &GetRange, len: u64) -> std::result::Result<Range<u64>, String> {
    match range {
        GetRange::Bounded(range) if range.start >= range.end => Err(format!(
            "Range started at {} and ended at {}",
            range.start, range.end
        )),
        GetRange::Bounded(range) if range.start >= len => Err(format!(
            "Wanted range starting at {}, but object was only {len} bytes long",
            range.start
        )),
        GetRange::Bounded(range) => Ok(range.start..range.end.min(len)),
        GetRange::Offset(offset) if *offset >= len => Err(format!(
            "Wanted range starting at {offset}, but object was only {len} bytes long"
        )),
        GetRange::Offset(offset) => Ok(*offset..len),
        GetRange::Suffix(count) => Ok(len.saturating_sub(*count)..len),
    }
}

fn check_preconditions(options: &GetOptions, meta: &ObjectMeta) -> Result<()> {
    let e_tag = meta.e_tag.as_deref().unwrap_or("*");
    let path = || meta.location.to_string();
    if let Some(expected) = &options.if_match {
        if expected != "*" && expected.split(',').map(str::trim).all(|tag| tag != e_tag) {
            return Err(Error::Precondition {
                path: path(),
                source: format!("{e_tag} does not match {expected}").into(),
            });
        }
    } else if let Some(date) = options.if_unmodified_since {
        if meta.last_modified > date {
            return Err(Error::Precondition {
                path: path(),
                source: format!("{date} < {}", meta.last_modified).into(),
            });
        }
    }
    if let Some(unexpected) = &options.if_none_match {
        if unexpected == "*" || unexpected.split(',').map(str::trim).any(|tag| tag == e_tag) {
            return Err(Error::NotModified {
                path: path(),
                source: format!("{e_tag} matches {unexpected}").into(),
            });
        }
    } else if let Some(date) = options.if_modified_since {
        if meta.last_modified <= date {
            return Err(Error::NotModified {
                path: path(),
                source: format!("{date} >= {}", meta.last_modified).into(),
            });
        }
    }
    Ok(())
}

impl HostObjectStore {
    pub fn new(host: Arc<HostFs>, root: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                host,
                root: root.into().trim_end_matches('/').to_string(),
                lock: Mutex::new(()),
            }),
        }
    }

    async fn run<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Inner) -> Result<T> + Send + 'static,
    {
        let inner = self.inner.clone();
        celld::asyncrt::blocking(move || {
            let _guard = inner.lock.lock().unwrap();
            operation(&inner)
        })
        .await
        .map_err(generic)?
    }
}

impl Inner {
    fn data_path(&self, location: &Path) -> String {
        format!("{}/{location}", self.root)
    }

    fn stored(&self, location: &Path) -> Result<Stored> {
        let data = self.data_path(location);
        let stat = self
            .host
            .stat(&data)
            .map_err(|error| not_found(location, error))?;
        if !stat.is_file {
            return Err(not_found(
                location,
                io::Error::new(io::ErrorKind::NotFound, "not an object"),
            ));
        }
        let sidecar = match self.host.read_file(&format!("{data}{META_SUFFIX}")) {
            Ok(bytes) => serde_json::from_slice::<Sidecar>(&bytes).map_err(generic)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Sidecar {
                e_tag: format!("{:x}-{:x}", stat.mtime_ms, stat.size),
                ..Sidecar::default()
            },
            Err(error) => return Err(generic(error)),
        };
        Ok(Stored {
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: timestamp(stat.mtime_ms),
                size: stat.size,
                e_tag: Some(sidecar.e_tag),
                version: None,
            },
            metadata: sidecar.metadata,
        })
    }

    fn write(
        &self,
        location: &Path,
        bytes: &[u8],
        metadata: BTreeMap<String, String>,
    ) -> Result<PutResult> {
        let data = self.data_path(location);
        if let Some((parent, _)) = data.rsplit_once('/') {
            self.host.mkdir_all(parent).map_err(generic)?;
        }
        let e_tag = new_e_tag();
        self.host.write_file(&data, bytes).map_err(generic)?;
        let sidecar = serde_json::to_vec(&Sidecar {
            e_tag: e_tag.clone(),
            metadata,
        })
        .map_err(generic)?;
        self.host
            .write_file(&format!("{data}{META_SUFFIX}"), &sidecar)
            .map_err(generic)?;
        Ok(PutResult {
            e_tag: Some(e_tag),
            version: None,
        })
    }

    fn put(
        &self,
        location: &Path,
        bytes: &[u8],
        mode: PutMode,
        metadata: BTreeMap<String, String>,
    ) -> Result<PutResult> {
        let current = match self.stored(location) {
            Ok(stored) => Some(stored),
            Err(Error::NotFound { .. }) => None,
            Err(error) => return Err(error),
        };
        match (&mode, &current) {
            (PutMode::Create, Some(_)) => {
                return Err(Error::AlreadyExists {
                    path: location.to_string(),
                    source: "object already exists".into(),
                })
            }
            (PutMode::Update(_), None) => {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: format!("Object at location {location} not found").into(),
                })
            }
            (PutMode::Update(expected), Some(stored)) if expected.e_tag != stored.meta.e_tag => {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: format!(
                        "ETag mismatch: expected {:?}, found {:?}",
                        expected.e_tag, stored.meta.e_tag
                    )
                    .into(),
                })
            }
            _ => {}
        }
        self.write(location, bytes, metadata)
    }

    fn walk(&self, directory: &str, relative: &str, out: &mut Vec<ObjectMeta>) -> Result<()> {
        let entries = match self.host.read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(generic(error)),
        };
        for entry in entries {
            let child = format!("{relative}{}", entry.name);
            if entry.is_dir {
                self.walk(
                    &format!("{directory}/{}", entry.name),
                    &format!("{child}/"),
                    out,
                )?;
            } else if !entry.name.ends_with(META_SUFFIX) {
                let location = Path::parse(&child).map_err(generic)?;
                out.push(self.stored(&location)?.meta);
            }
        }
        Ok(())
    }

    fn prefix_directory(&self, prefix: Option<&Path>) -> (String, String) {
        match prefix
            .map(|prefix| prefix.to_string())
            .filter(|p| !p.is_empty())
        {
            Some(prefix) => (format!("{}/{prefix}", self.root), format!("{prefix}/")),
            None => (self.root.clone(), String::new()),
        }
    }
}

impl Debug for HostObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{STORE}({})", self.inner.root)
    }
}

impl Display for HostObjectStore {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{STORE}({})", self.inner.root)
    }
}

#[async_trait::async_trait]
impl ObjectStore for HostObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let location = location.clone();
        let bytes = Bytes::from(payload);
        let metadata = metadata_of(&opts.attributes);
        self.run(move |inner| inner.put(&location, &bytes, opts.mode, metadata))
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(HostUpload {
            store: self.clone(),
            location: location.clone(),
            metadata: metadata_of(&opts.attributes),
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        self.run(move |inner| {
            let stored = inner.stored(&location)?;
            check_preconditions(&options, &stored.meta)?;
            let attributes = attributes_of(&stored.metadata);
            let size = stored.meta.size;
            let range = match &options.range {
                Some(range) => resolve_range(range, size).map_err(|message| Error::Generic {
                    store: STORE,
                    source: message.into(),
                })?,
                None => 0..size,
            };
            let bytes = if options.head {
                Bytes::new()
            } else {
                let data = inner
                    .host
                    .read_file(&inner.data_path(&location))
                    .map_err(|error| not_found(&location, error))?;
                let size = size as usize;
                Bytes::from(data).slice((range.start as usize).min(size)..(range.end as usize).min(size))
            };
            Ok(GetResult {
                payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
                meta: stored.meta,
                range,
                attributes,
            })
        })
        .await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let location = location.clone();
        self.run(move |inner| {
            let data = inner.data_path(&location);
            inner
                .host
                .unlink(&data)
                .map_err(|error| not_found(&location, error))?;
            let _ = inner.host.unlink(&format!("{data}{META_SUFFIX}"));
            Ok(())
        })
        .await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let store = self.clone();
        stream::once(async move {
            store
                .run(move |inner| {
                    let (directory, relative) = inner.prefix_directory(prefix.as_ref());
                    let mut objects = Vec::new();
                    inner.walk(&directory, &relative, &mut objects)?;
                    Ok(objects)
                })
                .await
        })
        .flat_map(|listed| match listed {
            Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.cloned();
        self.run(move |inner| {
            let (directory, relative) = inner.prefix_directory(prefix.as_ref());
            let entries = match inner.host.read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
                Err(error) => return Err(generic(error)),
            };
            let mut result = ListResult {
                common_prefixes: Vec::new(),
                objects: Vec::new(),
            };
            for entry in entries {
                let child = Path::parse(format!("{relative}{}", entry.name)).map_err(generic)?;
                if entry.is_dir {
                    result.common_prefixes.push(child);
                } else if !entry.name.ends_with(META_SUFFIX) {
                    result.objects.push(inner.stored(&child)?.meta);
                }
            }
            Ok(result)
        })
        .await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.run(move |inner| {
            let stored = inner.stored(&from)?;
            let bytes = inner
                .host
                .read_file(&inner.data_path(&from))
                .map_err(|error| not_found(&from, error))?;
            inner.write(&to, &bytes, stored.metadata).map(drop)
        })
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.run(move |inner| {
            if inner.stored(&to).is_ok() {
                return Err(Error::AlreadyExists {
                    path: to.to_string(),
                    source: "object already exists".into(),
                });
            }
            let stored = inner.stored(&from)?;
            let bytes = inner
                .host
                .read_file(&inner.data_path(&from))
                .map_err(|error| not_found(&from, error))?;
            inner.write(&to, &bytes, stored.metadata).map(drop)
        })
        .await
    }
}

struct HostUpload {
    store: HostObjectStore,
    location: Path,
    metadata: BTreeMap<String, String>,
    parts: Arc<Mutex<Vec<Bytes>>>,
}

impl Debug for HostUpload {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HostUpload")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl MultipartUpload for HostUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.parts.lock().unwrap().push(Bytes::from(data));
        Box::pin(async { Ok(()) })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let bytes = std::mem::take(&mut *self.parts.lock().unwrap()).concat();
        let location = self.location.clone();
        let metadata = self.metadata.clone();
        self.store
            .run(move |inner| inner.put(&location, &bytes, PutMode::Overwrite, metadata))
            .await
    }

    async fn abort(&mut self) -> Result<()> {
        self.parts.lock().unwrap().clear();
        Ok(())
    }
}

#[async_trait::async_trait]
impl PaginatedListStore for HostObjectStore {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default().to_string();
        let objects = self
            .run({
                let prefix = prefix.clone();
                move |inner| {
                    let directory = prefix
                        .rsplit_once('/')
                        .map(|(directory, _)| Path::from(directory));
                    let (directory, relative) = inner.prefix_directory(directory.as_ref());
                    let mut objects = Vec::new();
                    inner.walk(&directory, &relative, &mut objects)?;
                    Ok(objects)
                }
            })
            .await?;
        celld::file_store::paginate_listing(objects, &prefix, &options)
    }
}
