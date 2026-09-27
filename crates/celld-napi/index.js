// Copyright 2026 Deno Land Inc. Apache-2.0 license.
"use strict";

const crypto = require("node:crypto");
const nodeFs = require("node:fs");
const path = require("node:path");
const vfs = require("./vfs");
const { encode, decode } = require("./lib/codec.js");
const { prepareProject } = require("./lib/config.js");
const { createEnv } = require("./lib/proxies.js");

const native = require(process.env.CELLD_NAPI_PATH ?? path.join(__dirname, "celld.node"));

const DEFAULT_BUCKET = "/celld/bucket";
const DEFAULT_STATE = "/celld/state";
const BRIDGE_PATH = "/__celld_sdk__";
const BRIDGE_HEADER = "x-celld-sdk";
const SNAPSHOT_FORMAT = "celld-snapshot-v1";
// The stopped node's own fleet records: its registration, capacity lease, and
// drain token. A restore starts a new node, which would take them for a live
// peer's and wait on that peer before it could stop.
const NODE_RECORDS = /^(nodes\/|drain\/|fleet\/capacity-v1\.json)/;
// Fixed for the node's lifetime: they choose its storage and how it starts.
const NODE_OPTIONS = ["vfs", "persist", "bucket", "state", "snapshot", "esbuild", "celldEnv", "verbose"];

let claimed = false;

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

function openStorage(options) {
  if (options.persist !== undefined) {
    if (options.vfs !== undefined) throw new TypeError("give `persist` or `vfs`, not both");
    const root = path.resolve(options.persist);
    return { fs: nodeFs, bucket: path.join(root, "bucket"), state: path.join(root, "state") };
  }
  return {
    fs: options.vfs ?? vfs.create(new vfs.MemoryProvider(), { emitExperimentalWarning: false }),
    bucket: options.bucket ?? DEFAULT_BUCKET,
    state: options.state ?? DEFAULT_STATE,
  };
}

function* walk(fs, dir, relative = "") {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const name = relative ? `${relative}/${entry.name}` : entry.name;
    if (entry.isDirectory()) yield* walk(fs, path.join(dir, entry.name), name);
    else yield name;
  }
}

function resolveInput(input, base) {
  const target = new URL(input instanceof Request ? input.url : input, base);
  return new URL(target.pathname + target.search, base);
}

/**
 * A celld node in this process, running one Worker. See index.d.ts.
 */
class Celld {
  #options;
  #storage;
  #spec;
  #esbuild;
  #token;
  #config;
  #project = null;
  #env = null;
  #listening = null;
  #exit = null;
  #exited;
  #stopping = null;
  #deploying = Promise.resolve();
  #ready;

  constructor(options = {}) {
    if (claimed) throw new Error("this process already runs a celld node, and a process runs one for its lifetime");
    const snapshot = options.snapshot;
    if (snapshot !== undefined && snapshot?.format !== SNAPSHOT_FORMAT) {
      throw new TypeError(`options.snapshot is not a ${SNAPSHOT_FORMAT} snapshot`);
    }
    this.#storage = openStorage(options);
    this.#options = options;
    this.#spec = `file://${this.#storage.bucket}`;
    this.#esbuild = options.esbuild ?? findEsbuild();
    const hasWorker = ["script", "scriptPath", "project"].some((key) => options[key] !== undefined);
    if (hasWorker) {
      this.#token = crypto.randomBytes(24).toString("base64url");
      this.#project = prepareProject(options, this.#token);
      this.#config = this.#project.config;
    } else if (snapshot !== undefined) {
      this.#token = snapshot.token;
      this.#config = snapshot.config;
    } else {
      throw new TypeError("give the Worker as `script`, `scriptPath`, or `project`, or restore a `snapshot`");
    }
    claimed = true;
    let settle;
    this.#exited = new Promise((resolve) => (settle = resolve));
    this.#ready = this.#start(snapshot, (code, message) => {
      this.#exit = { code, message };
      this.#project?.dispose();
      settle(this.#exit);
    });
    this.#ready.catch(() => this.#project?.dispose());
  }

  /** Construct a node and wait until it serves. */
  static async start(options) {
    const celld = new Celld(options);
    await celld.ready;
    return celld;
  }

  async #start(snapshot, onExit) {
    const { fs, bucket, state } = this.#storage;
    fs.mkdirSync(bucket, { recursive: true });
    fs.mkdirSync(state, { recursive: true });
    if (snapshot !== undefined) {
      if (fs.readdirSync(bucket).length > 0) {
        throw new Error(`cannot restore a snapshot into ${bucket}, which is not empty`);
      }
      for (const [name, base64] of Object.entries(snapshot.files)) {
        const file = path.join(bucket, ...name.split("/"));
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, Buffer.from(base64, "base64"));
      }
    }
    native.installHostStorage(dispatcher(fs), bucket, state);
    native.setQuiet(!this.#options.verbose);
    if (this.#project) await native.deploy([this.#project.configPath, "--bucket", this.#spec], this.#esbuild);
    // A graceful stop hands each cell to a successor node and waits for the
    // adoption. An embedded node has no peers, so it would sit out the whole
    // no-progress window (25 s at celld's default budget) after its cells are
    // already durable in the bucket and released.
    const variables = { RUST_LOG: "warn", CELLD_SHUTDOWN_TOTAL_MS: "3000", ...this.#options.celldEnv };
    const listening = await native.start(
      ["--bucket", this.#spec, "--listen", "127.0.0.1:0", "--internal-listen", "127.0.0.1:0", "--no-control-plane"],
      variables,
      onExit,
    );
    this.#listening = {
      url: `http://${listening.publicAddress}`,
      internalUrl: `http://${listening.internalAddress}`,
    };
    return new URL(this.#listening.url);
  }

  /** Settles with the node's public URL once it serves. */
  get ready() {
    return this.#ready;
  }

  #listener() {
    if (this.#listening === null) throw new Error("the node is not serving yet; await `ready` first");
    return this.#listening;
  }

  get url() {
    return this.#listener().url;
  }

  get internalUrl() {
    return this.#listener().internalUrl;
  }

  get vfs() {
    return this.#storage.fs;
  }

  get bucket() {
    return this.#storage.bucket;
  }

  get state() {
    return this.#storage.state;
  }

  get exited() {
    return this.#exited;
  }

  async fetch(input, init) {
    await this.#ready;
    if (this.#exit) throw new Error(`celld has stopped (code ${this.#exit.code})`);
    const url = resolveInput(input, this.#listening.url);
    if (!(input instanceof Request)) return fetch(url, init);
    const body = input.body === null ? undefined : await input.arrayBuffer();
    return fetch(url, {
      method: input.method,
      headers: input.headers,
      body,
      redirect: input.redirect,
      signal: input.signal,
      ...init,
    });
  }

  dispatchFetch(input, init) {
    return this.fetch(input, init);
  }

  #call = async (op) => {
    const response = await this.fetch(BRIDGE_PATH, {
      method: "POST",
      headers: { [BRIDGE_HEADER]: this.#token, "content-type": "application/json" },
      body: JSON.stringify(await encode(op)),
    });
    const text = await response.text();
    let reply;
    try {
      reply = JSON.parse(text);
    } catch {}
    if (typeof reply?.ok !== "boolean") {
      throw new Error(`the Worker did not answer the binding bridge (HTTP ${response.status}): ${text.slice(0, 200)}`);
    }
    if (!reply.ok) throw decode(reply.error);
    return decode(reply.value);
  };

  /** The Worker's bindings, callable from this process. */
  get env() {
    this.#env ??= createEnv(this.#config, this.#call);
    return this.#env;
  }

  async getBindings() {
    await this.#ready;
    return this.env;
  }

  async #binding(name) {
    const env = await this.getBindings();
    if (!(name in env)) throw new ReferenceError(`the Worker has no binding named ${name}`);
    return env[name];
  }

  getDurableObjectNamespace(binding) {
    return this.#binding(binding);
  }

  getD1Database(binding) {
    return this.#binding(binding);
  }

  getKVNamespace(binding) {
    return this.#binding(binding);
  }

  getR2Bucket(binding) {
    return this.#binding(binding);
  }

  getQueueProducer(binding) {
    return this.#binding(binding);
  }

  /** Deploy `options` in place of the current ones, as Miniflare's setOptions does. */
  setOptions(options) {
    return this.#deploy(options);
  }

  /** Deploy the current options with `overrides` applied; `bindings` merge. */
  redeploy(overrides = {}) {
    const current = this.#options;
    const bindings = overrides.bindings === undefined ? current.bindings : { ...current.bindings, ...overrides.bindings };
    const sources = ["script", "scriptPath", "project"];
    const base = sources.some((key) => overrides[key] !== undefined)
      ? Object.fromEntries(Object.entries(current).filter(([key]) => !sources.includes(key)))
      : current;
    return this.#deploy({ ...base, ...overrides, bindings });
  }

  #deploy(options) {
    const run = this.#deploying.then(async () => {
      await this.#ready;
      if (this.#exit) throw new Error(`celld has stopped (code ${this.#exit.code})`);
      for (const key of NODE_OPTIONS) {
        const [next, now] = [options[key], this.#options[key]];
        const same = key === "celldEnv" ? JSON.stringify(next) === JSON.stringify(now) : next === now;
        if (next !== undefined && !same) throw new TypeError(`${key} is fixed when the node starts`);
      }
      const project = prepareProject(options, this.#token);
      try {
        await native.deploy([project.configPath, "--bucket", this.#spec], this.#esbuild);
        const response = await fetch(`${this.#listening.internalUrl}/reload`, {
          method: "POST",
          signal: AbortSignal.timeout(30_000),
        });
        const reply = await response.text();
        if (!response.ok) throw new Error(`celld refused the reload (HTTP ${response.status}): ${reply}`);
      } catch (error) {
        project.dispose();
        throw error;
      }
      this.#project?.dispose();
      this.#project = project;
      this.#config = project.config;
      this.#env = null;
      this.#options = { ...options, ...Object.fromEntries(NODE_OPTIONS.map((key) => [key, this.#options[key]])) };
    });
    this.#deploying = run.catch(() => {});
    return run;
  }

  /**
   * The fleet bucket's files, from which `new Celld({ snapshot })` restores
   * every cell and the deployment. Only a stopped node can be copied: a
   * running node holds a lease on each of its cells.
   */
  async snapshot() {
    await this.#ready.catch(() => {});
    if (!this.#exit) {
      throw new Error("stop the node before taking a snapshot: a running node holds a lease on each of its cells");
    }
    const { fs, bucket } = this.#storage;
    const files = {};
    for (const name of walk(fs, bucket)) {
      if (NODE_RECORDS.test(name)) continue;
      files[name] = fs.readFileSync(path.join(bucket, ...name.split("/"))).toString("base64");
    }
    return { format: SNAPSHOT_FORMAT, token: this.#token, config: this.#config, files };
  }

  /** Hand every cell back to the bucket and stop, as SIGTERM does for the binary. */
  async stop() {
    try {
      await this.#ready;
    } catch {
      return { code: 1, message: "the node did not start" };
    }
    if (this.#exit) return this.#exit;
    this.#stopping ??= fetch(`${this.#listening.internalUrl}/shutdown`, {
      method: "POST",
      signal: AbortSignal.timeout(10_000),
    }).then((response) => {
      if (!response.ok) throw new Error(`celld refused shutdown: ${response.status}`);
    });
    await this.#stopping;
    return this.#exited;
  }

  async dispose() {
    await this.stop();
  }

  [Symbol.asyncDispose]() {
    return this.dispose();
  }
}

/** Start the process's celld node. Equivalent to `Celld.start(options)`. */
function startCelld(options) {
  return Celld.start(options);
}

module.exports = { Celld, startCelld, vfs };
