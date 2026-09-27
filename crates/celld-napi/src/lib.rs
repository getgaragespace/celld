// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! A celld node inside a Bun or Node process.
//!
//! The node is the standalone binary's own code: `main.rs` is compiled here as
//! a module and driven through [`celld::embed`]. It runs on celld's V8 and
//! its own Tokio runtime, and it serves HTTP on loopback listeners. What the
//! host supplies is storage: a JavaScript virtual filesystem holds the fleet
//! bucket, every SQLite database, and every other file the node writes.
//!
//! Call order: `installHostStorage`, then `deploy` (optional), then `start`.
//! A process runs at most one node.

mod bridge;
mod host_fs;
mod object_store;
mod sqlite_vfs;

#[path = "../../celld/main.rs"]
#[allow(dead_code, unused_imports)]
mod node;

use bridge::HostFs;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{Env, JsDeferred, JsFunction, JsObject};
use napi_derive::napi;
use std::sync::{Arc, Mutex, OnceLock};

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

/// celld's runtime. It is never dropped: tearing it down under live isolates
/// is what the standalone binary avoids by exiting instead of returning.
fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
        // Before the runtime's threads exist, as in the standalone binary.
        celld::runtime::init_v8();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("celld")
            .build()
            .expect("build the celld runtime");
        celld::asyncrt::set_host_handle(runtime.handle().clone());
        runtime
    })
}

fn error(error: impl std::fmt::Display) -> napi::Error {
    napi::Error::from_reason(error.to_string())
}

/// Drive a future on its own thread with `block_on`, as the standalone binary
/// drives `async_main`: the node's futures are not `Send`, so they cannot be
/// spawned onto the runtime. The thread starts after V8, so it inherits
/// V8's memory-protection key.
fn block_on_thread<F, Fut>(name: &str, make: F) -> Result<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()>,
{
    let runtime = runtime();
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || runtime.block_on(make()))
        .map(drop)
        .map_err(error)
}

/// Serve every file the node writes from the host filesystem.
///
/// `dispatch(op, ...args)` performs one filesystem operation and returns
/// `[null, value]` or `[code, message]`; `index.js` implements it over a
/// `node:vfs` VirtualFileSystem. The fleet bucket is the `file://` root
/// `bucketRoot`, and the node's local state lives under `stateRoot`. Both are
/// absolute paths in the host filesystem.
#[napi]
pub fn install_host_storage(
    env: Env,
    dispatch: JsFunction,
    bucket_root: String,
    state_root: String,
) -> Result<()> {
    if RUNTIME.get().is_some() {
        return Err(error(
            "host storage must be installed before the node starts",
        ));
    }
    let host = Arc::new(HostFs::new(&env, dispatch)?);
    celld::asyncrt::install_filesystem(Arc::new(host_fs::NodeFileSystem::new(host.clone())), 255)
        .map_err(error)?;
    sqlite_vfs::install(host.clone()).map_err(error)?;
    celld::file_store::register_host_store(
        bucket_root.clone(),
        Arc::new(object_store::HostObjectStore::new(host, bucket_root)),
    );
    std::env::set_var("CELLD_WATCH", state_root);
    Ok(())
}

/// Whether deploys and the node print their command-line output (the deploy
/// report, the listener lines). Warnings and errors go through `RUST_LOG`
/// either way.
#[napi]
pub fn set_quiet(quiet: bool) {
    celld::embed::set_quiet(quiet);
}

type Deferred<T> = JsDeferred<T, Box<dyn FnOnce(Env) -> Result<T> + Send>>;

/// Deploy a Wrangler project into the fleet bucket, as `celld deploy` does.
/// `arguments` are the `celld deploy` arguments; `esbuild` is the bundler
/// executable for Worker code.
#[napi(ts_return_type = "Promise<void>")]
pub fn deploy(env: Env, arguments: Vec<String>, esbuild: Option<String>) -> Result<JsObject> {
    if let Some(esbuild) = esbuild {
        std::env::set_var("CELLD_ESBUILD", esbuild);
    }
    let (deferred, promise): (Deferred<()>, JsObject) = env.create_deferred()?;
    block_on_thread("celld-deploy", move || async move {
        match celld::fleet::run_deploy(arguments).await {
            Ok(()) => deferred.resolve(Box::new(|_| Ok(()))),
            Err(failure) => deferred.reject(error(format!("{failure:#}"))),
        }
    })?;
    Ok(promise)
}

#[napi(object)]
pub struct Listening {
    pub public_address: String,
    pub internal_address: String,
}

/// Run the node with `arguments` (the standalone command line after the
/// program name) and the `CELLD_*` or `RUST_LOG` settings in `variables`.
/// Resolves once both listeners are bound. `onExit(code, message)` runs when
/// the node stops, after a graceful `POST /shutdown` or a failure.
#[napi(ts_return_type = "Promise<Listening>")]
pub fn start(
    env: Env,
    arguments: Vec<String>,
    variables: std::collections::HashMap<String, String>,
    on_exit: JsFunction,
) -> Result<JsObject> {
    for (name, value) in variables {
        std::env::set_var(name, value);
    }
    celld::env_vars::validate().map_err(error)?;
    let (deferred, promise): (Deferred<Listening>, JsObject) = env.create_deferred()?;
    let ready = Arc::new(Mutex::new(Some(deferred)));
    let mut exit: ThreadsafeFunction<(i32, String), ErrorStrategy::Fatal> = on_exit
        .create_threadsafe_function(0, |context: ThreadSafeCallContext<(i32, String)>| {
            let (code, message) = context.value;
            Ok(vec![
                context.env.create_int32(code)?.into_unknown(),
                context.env.create_string(&message)?.into_unknown(),
            ])
        })?;
    exit.unref(&env)?;
    let report_exit = {
        let ready = ready.clone();
        let exit = exit.clone();
        move |code: i32, message: String| {
            if let Some(deferred) = ready.lock().unwrap().take() {
                deferred.reject(error(format!(
                    "celld stopped before it listened (code {code}): {message}"
                )));
            }
            exit.call((code, message), ThreadsafeFunctionCallMode::NonBlocking);
        }
    };
    celld::embed::install(celld::embed::Embedding {
        arguments,
        on_listening: Box::new({
            let ready = ready.clone();
            move |public, internal| {
                if let Some(deferred) = ready.lock().unwrap().take() {
                    deferred.resolve(Box::new(move |_| {
                        Ok(Listening {
                            public_address: public.to_string(),
                            internal_address: internal.to_string(),
                        })
                    }));
                }
            }
        }),
        on_exit: Box::new({
            let report_exit = report_exit.clone();
            move |code| report_exit(code, String::new())
        }),
    })
    .map_err(error)?;
    block_on_thread("celld-node", move || async move {
        // The node returns only when it fails before serving, or for a
        // command that is not a node; every other end goes through `on_exit`.
        match node::async_main(None).await {
            Ok(()) => report_exit(0, "the arguments did not start a node".to_string()),
            Err(failure) => report_exit(1, format!("{failure:#}")),
        }
    })?;
    Ok(promise)
}
