// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Hooks for a node that runs inside a host process instead of owning one.
//!
//! The standalone binary never installs an [`Embedding`], so every hook below
//! is inert there. A host (the Node-API addon) installs one before it runs the
//! node: the node then takes its arguments from the host, reports its bound
//! listeners, leaves the host's signal handlers alone, and reports an exit
//! code instead of ending the host process.

use std::net::SocketAddr;
use std::sync::OnceLock;

type ListeningHook = Box<dyn Fn(SocketAddr, SocketAddr) + Send + Sync>;
type ExitHook = Box<dyn Fn(i32) + Send + Sync>;

pub struct Embedding {
    /// The command line after the program name, as `std::env::args` would
    /// give it to the standalone binary.
    pub arguments: Vec<String>,
    /// Called once with the public and internal listener addresses.
    pub on_listening: ListeningHook,
    /// Called with the code the standalone binary would exit with. The node
    /// is finished when this runs; its threads are parked, not torn down,
    /// because dropping live isolates under the V8 platform is not safe.
    pub on_exit: ExitHook,
}

static EMBEDDING: OnceLock<Embedding> = OnceLock::new();

/// Install the host's hooks. A process holds at most one embedded node.
pub fn install(embedding: Embedding) -> anyhow::Result<()> {
    EMBEDDING
        .set(embedding)
        .map_err(|_| anyhow::anyhow!("this process already embeds a celld node"))
}

pub fn get() -> Option<&'static Embedding> {
    EMBEDDING.get()
}
