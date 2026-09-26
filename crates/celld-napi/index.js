// Copyright 2026 Deno Land Inc. Apache-2.0 license.
"use strict";

const path = require("node:path");
const vfs = require("./vfs");

const native = require(process.env.CELLD_NAPI_PATH ?? path.join(__dirname, "celld.node"));

const DEFAULT_BUCKET = "/celld/bucket";
const DEFAULT_STATE = "/celld/state";

let started = false;

/** Answer the addon's filesystem calls from a VirtualFileSystem. */
function dispatcher(fs) {
  const stat = (s) => ({ size: s.size, dir: s.isDirectory(), file: s.isFile(), mtimeMs: s.mtimeMs });
  const ops = {
    open: (file, flags) => fs.openSync(file, flags),
    close: (fd) => fs.closeSync(fd),
    read(fd, position, length) {
      const buffer = Buffer.alloc(length);
      const read = fs.readSync(fd, buffer, 0, length, position);
      return read === length ? buffer : buffer.subarray(0, read);
    },
    write: (fd, position, data) => fs.writeSync(fd, data, 0, data.length, position),
    truncate: (fd, length) => fs.ftruncateSync(fd, length),
    fstat: (fd) => stat(fs.fstatSync(fd)),
    stat: (file) => stat(fs.statSync(file)),
    readFile: (file) => fs.readFileSync(file),
    writeFile: (file, data) => fs.writeFileSync(file, data),
    readdir: (dir) =>
      fs.readdirSync(dir, { withFileTypes: true }).map((entry) =>
        entry.isDirectory() ? `${entry.name}/` : entry.name),
    mkdirAll: (dir) => fs.mkdirSync(dir, { recursive: true }),
    rename: (from, to) => fs.renameSync(from, to),
    unlink: (file) => fs.unlinkSync(file),
    rmAll: (dir) => fs.rmSync(dir, { recursive: true, force: true }),
  };
  return (op, a, b, c) => {
    try {
      return [null, ops[op](a, b, c)];
    } catch (error) {
      return [error?.code ?? "EIO", String(error?.message ?? error)];
    }
  };
}

function findEsbuild() {
  try {
    return require.resolve(`@esbuild/${process.platform}-${process.arch}/bin/esbuild`);
  } catch {
    return undefined;
  }
}

/**
 * Start a celld node in this process with its storage in a VirtualFileSystem.
 * See index.d.ts.
 */
async function startCelld(options = {}) {
  if (started) throw new Error("this process already runs a celld node");
  started = true;
  const fs = options.vfs ?? vfs.create(new vfs.MemoryProvider(), { emitExperimentalWarning: false });
  const bucket = options.bucket ?? DEFAULT_BUCKET;
  const state = options.state ?? DEFAULT_STATE;
  fs.mkdirSync(bucket, { recursive: true });
  fs.mkdirSync(state, { recursive: true });
  native.installHostStorage(dispatcher(fs), bucket, state);

  const bucketSpec = `file://${bucket}`;
  if (options.project) {
    const esbuild = options.esbuild ?? findEsbuild();
    await native.deploy([path.resolve(options.project), "--bucket", bucketSpec], esbuild);
  }

  // A stopped node keeps its listeners bound but no longer accepts on them,
  // so requests after the exit must not reach the sockets.
  let exit = null;
  let resolveExit;
  const exited = new Promise((resolve) => (resolveExit = resolve));
  const variables = { RUST_LOG: "warn", ...options.env };
  const listening = await native.start(
    ["--bucket", bucketSpec, "--listen", "127.0.0.1:0", "--internal-listen", "127.0.0.1:0", "--no-control-plane"],
    variables,
    (code, message) => resolveExit((exit = { code, message })),
  );
  const url = `http://${listening.publicAddress}`;
  const internalUrl = `http://${listening.internalAddress}`;
  let stopping = null;
  return {
    url,
    internalUrl,
    vfs: fs,
    bucket,
    state,
    exited,
    fetch(input, init) {
      if (exit) return Promise.reject(new Error(`celld has stopped (code ${exit.code})`));
      return fetch(new URL(input, url), init);
    },
    async stop() {
      if (exit) return exit;
      stopping ??= fetch(`${internalUrl}/shutdown`, { method: "POST", signal: AbortSignal.timeout(10_000) })
        .then((response) => {
          if (!response.ok) throw new Error(`celld refused shutdown: ${response.status}`);
        });
      await stopping;
      return exited;
    },
  };
}

module.exports = { startCelld, vfs };
