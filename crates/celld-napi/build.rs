// Cargo reads build-script directives from stdout.
#[allow(clippy::disallowed_macros)]
fn main() {
    napi_build::setup();
    // `main.rs`, compiled into this crate, leaves out what only a standalone
    // process may do.
    println!("cargo:rustc-cfg=celld_embed");
}
