// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Local filesystem object store with conditional writes for `file://` fleets.
//!
//! [`object_store::local::LocalFileSystem`] supports `PutMode::Create` but returns
//! `NotImplemented` for `PutMode::Update` and for user metadata. celld's ownership
//! fence and LTX replica lane need both, so [`ConditionalLocalFileSystem`]
//! implements update-by-etag under an advisory lock and stores metadata in a
//! sidecar file next to each object.

use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::TryStreamExt as _;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, Error, GetOptions, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult,
};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::ops::Range;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

/// Advisory lock file name in the fleet root; serializes conditional writes
/// across processes sharing one `file://` directory.
const LOCK_FILE: &str = ".celld-cas.lock";
const META_SUFFIX: &str = ".celld-meta.json";

struct AdvisoryLock {
    _file: File,
}

impl AdvisoryLock {
    fn acquire(root: &FsPath) -> Result<Self, Error> {
        let lock_path = root.join(LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|source| Error::Generic {
                store: "ConditionalLocalFileSystem",
                source: Box::new(source),
            })?;
        file.lock().map_err(|source| Error::Generic {
            store: "ConditionalLocalFileSystem",
            source: Box::new(source),
        })?;
        Ok(Self { _file: file })
    }
}

fn meta_path_for(data_path: &FsPath) -> PathBuf {
    let mut sidecar = data_path.as_os_str().to_owned();
    sidecar.push(META_SUFFIX);
    PathBuf::from(sidecar)
}

fn attributes_to_map(attributes: &Attributes) -> BTreeMap<String, String> {
    attributes
        .iter()
        .filter_map(|(key, value)| match key {
            Attribute::Metadata(name) => Some((name.to_string(), value.as_ref().to_string())),
            _ => None,
        })
        .collect()
}

fn map_to_attributes(map: BTreeMap<String, String>) -> Attributes {
    let mut attributes = Attributes::new();
    for (name, value) in map {
        attributes.insert(Attribute::Metadata(name.into()), value.into());
    }
    attributes
}

fn write_meta_sidecar(data_path: &FsPath, attributes: &Attributes) -> Result<(), Error> {
    let map = attributes_to_map(attributes);
    if map.is_empty() {
        let _ = std::fs::remove_file(meta_path_for(data_path));
        return Ok(());
    }
    let bytes = serde_json::to_vec(&map).map_err(|source| Error::Generic {
        store: "ConditionalLocalFileSystem",
        source: Box::new(source),
    })?;
    std::fs::write(meta_path_for(data_path), bytes).map_err(|source| Error::Generic {
        store: "ConditionalLocalFileSystem",
        source: Box::new(source),
    })
}

fn read_meta_sidecar(data_path: &FsPath) -> Result<Attributes, Error> {
    let bytes = match std::fs::read(meta_path_for(data_path)) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Attributes::default());
        }
        Err(source) => {
            return Err(Error::Generic {
                store: "ConditionalLocalFileSystem",
                source: Box::new(source),
            });
        }
    };
    let map: BTreeMap<String, String> =
        serde_json::from_slice(&bytes).map_err(|source| Error::Generic {
            store: "ConditionalLocalFileSystem",
            source: Box::new(source),
        })?;
    Ok(map_to_attributes(map))
}

#[derive(Clone)]
pub(crate) struct ConditionalLocalFileSystem {
    inner: Arc<LocalFileSystem>,
    root: PathBuf,
}

impl Debug for ConditionalLocalFileSystem {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ConditionalLocalFileSystem({})", self.root.display())
    }
}

impl ConditionalLocalFileSystem {
    /// Opens a local fleet root, creating it when absent.
    pub(crate) fn open(root: impl AsRef<FsPath>) -> Result<Arc<Self>, Error> {
        let root = root.as_ref();
        std::fs::create_dir_all(root).map_err(|source| Error::Generic {
            store: "ConditionalLocalFileSystem",
            source: Box::new(source),
        })?;
        let inner = Arc::new(LocalFileSystem::new_with_prefix(root)?);
        Ok(Arc::new(Self {
            inner,
            root: root.to_path_buf(),
        }))
    }

    fn persist_attributes(&self, location: &Path, attributes: &Attributes) -> Result<(), Error> {
        if attributes.is_empty() {
            return Ok(());
        }
        let data_path = self.inner.path_to_filesystem(location)?;
        write_meta_sidecar(&data_path, attributes)
    }

    fn load_attributes(&self, location: &Path) -> Result<Attributes, Error> {
        let data_path = self.inner.path_to_filesystem(location)?;
        read_meta_sidecar(&data_path)
    }
}

impl Display for ConditionalLocalFileSystem {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ConditionalLocalFileSystem({})", self.root.display())
    }
}

struct MetaMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    store: ConditionalLocalFileSystem,
    location: Path,
    attributes: Attributes,
}

impl Debug for MetaMultipartUpload {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetaMultipartUpload")
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl MultipartUpload for MetaMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult, Error> {
        let result = self.inner.complete().await?;
        self.store
            .persist_attributes(&self.location, &self.attributes)?;
        Ok(result)
    }

    async fn abort(&mut self) -> Result<(), Error> {
        self.inner.abort().await
    }
}

#[async_trait::async_trait]
impl ObjectStore for ConditionalLocalFileSystem {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, Error> {
        let attributes = opts.attributes.clone();
        let mut opts = opts;
        opts.attributes = Attributes::new();

        let result = if let PutMode::Update(expected) = &opts.mode {
            let expected = expected.clone();
            let _guard = tokio::task::spawn_blocking({
                let root = self.root.clone();
                move || AdvisoryLock::acquire(&root)
            })
            .await
            .map_err(|source| Error::Generic {
                store: "ConditionalLocalFileSystem",
                source: Box::new(source),
            })??;

            let current = self.inner.head(location).await?;
            if current.e_tag != expected.e_tag {
                return Err(Error::Precondition {
                    path: location.to_string(),
                    source: format!(
                        "ETag mismatch: expected {:?}, found {:?}",
                        expected.e_tag, current.e_tag
                    )
                    .into(),
                });
            }
            let overwrite = PutOptions {
                mode: PutMode::Overwrite,
                ..opts
            };
            self.inner.put_opts(location, payload, overwrite).await?
        } else {
            self.inner.put_opts(location, payload, opts).await?
        };

        self.persist_attributes(location, &attributes)?;
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, Error> {
        let attributes = opts.attributes.clone();
        let mut opts = opts;
        opts.attributes = Attributes::new();
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(MetaMultipartUpload {
            inner,
            store: self.clone(),
            location: location.clone(),
            attributes,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult, Error> {
        let mut result = self.inner.get_opts(location, options).await?;
        result.attributes = self.load_attributes(location)?;
        Ok(result)
    }

    async fn delete(&self, location: &Path) -> Result<(), Error> {
        if let Ok(data_path) = self.inner.path_to_filesystem(location) {
            let _ = std::fs::remove_file(meta_path_for(&data_path));
        }
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta, Error>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult, Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<(), Error> {
        self.inner.copy(from, to).await?;
        self.persist_attributes(to, &self.load_attributes(from)?)
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<(), Error> {
        self.inner.copy_if_not_exists(from, to).await?;
        self.persist_attributes(to, &self.load_attributes(from)?)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>, Error> {
        self.inner.get_ranges(location, ranges).await
    }
}

#[async_trait::async_trait]
impl PaginatedListStore for ConditionalLocalFileSystem {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> Result<PaginatedListResult, Error> {
        let prefix = prefix.unwrap_or_default();
        let directory = prefix.rsplit_once('/').map(|(directory, _)| Path::from(directory));
        let objects: Vec<ObjectMeta> = self.inner.list(directory.as_ref()).try_collect().await?;
        paginate_listing(objects, prefix, &options)
    }
}

/// Paginated listing for a store that can only list everything under a
/// prefix.
pub struct ListedPages(pub Arc<dyn ObjectStore>);

#[async_trait::async_trait]
impl PaginatedListStore for ListedPages {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> Result<PaginatedListResult, Error> {
        let prefix = prefix.unwrap_or_default();
        let directory = prefix.rsplit_once('/').map(|(directory, _)| Path::from(directory));
        let objects: Vec<ObjectMeta> = self.0.list(directory.as_ref()).try_collect().await?;
        paginate_listing(objects, prefix, &options)
    }
}

/// One page of a key-ordered listing, as a paginated store answers it:
/// keys after the page token (or offset), the keys below a delimiter folded
/// into their common prefix, and a page token when `max_keys` cut the page.
/// For stores that can only list everything under a prefix.
pub fn paginate_listing(
    mut objects: Vec<ObjectMeta>,
    prefix: &str,
    options: &PaginatedListOptions,
) -> Result<PaginatedListResult, Error> {
    objects.retain(|object| {
        let key = object.location.as_ref();
        key.starts_with(prefix) && !key.ends_with(META_SUFFIX) && key != LOCK_FILE
    });
    objects.sort_by(|left, right| left.location.as_ref().cmp(right.location.as_ref()));
    let after = options.page_token.as_deref().or(options.offset.as_deref());
    let delimiter = options.delimiter.as_deref();
    let limit = options.max_keys.unwrap_or(usize::MAX);
    let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
    for object in objects {
        let key = object.location.as_ref().to_string();
        if after.is_some_and(|after| key.as_str() <= after) {
            continue;
        }
        let remainder = &key[prefix.len()..];
        let common = delimiter
            .and_then(|delimiter| {
                remainder
                    .find(delimiter)
                    .map(|index| Path::parse(format!("{prefix}{}{delimiter}", &remainder[..index])))
            })
            .transpose()?;
        match common {
            Some(common) => {
                if let Some((last, Some(previous), _)) = entries.last_mut() {
                    if *previous == common {
                        *last = key;
                        continue;
                    }
                }
                entries.push((key, Some(common), None));
            }
            None => entries.push((key, None, Some(object))),
        }
    }
    let truncated = entries.len() > limit;
    entries.truncate(limit);
    let page_token = truncated
        .then(|| entries.last().map(|(last, _, _)| last.clone()))
        .flatten();
    let mut result = ListResult {
        common_prefixes: Vec::new(),
        objects: Vec::new(),
    };
    for (_, common, object) in entries {
        result.common_prefixes.extend(common);
        result.objects.extend(object);
    }
    Ok(PaginatedListResult { result, page_token })
}
