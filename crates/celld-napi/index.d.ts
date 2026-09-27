/** The subset of `node:vfs`'s VirtualFileSystem that the node uses. */
export interface VirtualFileSystem {
  openSync(path: string, flags: string): number;
  closeSync(fd: number): void;
  readSync(fd: number, buffer: Buffer, offset: number, length: number, position: number): number;
  writeSync(fd: number, buffer: Buffer, offset: number, length: number, position: number): number;
  ftruncateSync(fd: number, length: number): void;
  fstatSync(fd: number): { size: number; mtimeMs: number; isFile(): boolean; isDirectory(): boolean };
  statSync(path: string): { size: number; mtimeMs: number; isFile(): boolean; isDirectory(): boolean };
  readFileSync(path: string): Buffer;
  writeFileSync(path: string, data: Uint8Array | string): void;
  readdirSync(path: string): string[];
  readdirSync(path: string, options: { withFileTypes: true }): Array<{ name: string; isDirectory(): boolean }>;
  mkdirSync(path: string, options: { recursive: true }): unknown;
  renameSync(from: string, to: string): void;
  unlinkSync(path: string): void;
  rmSync(path: string, options: { recursive: true; force: true }): void;
  existsSync(path: string): boolean;
}

/** A binding name mapped to a resource name, or binding names that double as resource names. */
export type ResourceBindings = string[] | Record<string, string>;

export interface QueueConsumerOptions {
  maxBatchSize?: number;
  /** Seconds, 0 to 60. */
  maxBatchTimeout?: number;
  maxRetries?: number;
  deadLetterQueue?: string;
  maxConcurrency?: number;
  /** Seconds. */
  retryDelay?: number;
}

/**
 * The Worker and its bindings, in Miniflare's option names. Give the Worker
 * as exactly one of `script`, `scriptPath`, or `project`. With `project`,
 * the Wrangler config supplies the bindings and these options add to them.
 */
export interface WorkerOptions {
  /** A Wrangler project directory, or its wrangler.jsonc / wrangler.json. */
  project?: string;
  /** The Worker's main module source. It cannot import relative modules. */
  script?: string;
  /** The Worker's main module; esbuild bundles what it imports. */
  scriptPath?: string;
  /** Ignored when true; celld runs ES module Workers only. */
  modules?: boolean;
  /** The Worker's name. Durable Object IDs derive from it. Default `worker`. */
  name?: string;
  compatibilityDate?: string;
  compatibilityFlags?: string[];
  /** Vars. celld's vars are plain text, so every value is a string. */
  bindings?: Record<string, string>;
  /** Binding to class. Every class is SQLite-backed. */
  durableObjects?: Record<string, string | { className: string; scriptName?: string }>;
  /** Binding to database name. */
  d1Databases?: ResourceBindings;
  /** Binding to namespace id. */
  kvNamespaces?: ResourceBindings;
  /** Binding to bucket name. */
  r2Buckets?: ResourceBindings;
  /** Binding to queue name. */
  queueProducers?: string[] | Record<string, string | { queueName: string; deliveryDelay?: number }>;
  /** Queue name to consumer settings. */
  queueConsumers?: string[] | Record<string, QueueConsumerOptions>;
  /** Binding to a Worker on this node, optionally one of its entrypoints. */
  serviceBindings?: Record<string, string | { name: string; entrypoint?: string }>;
  workflows?: Record<string, { name: string; className: string; scriptName?: string }>;
  crons?: string[];
  assets?: {
    /** Resolved against the working directory. The SDK deploys a copy. */
    directory: string;
    binding?: string;
    htmlHandling?: "auto-trailing-slash" | "force-trailing-slash" | "drop-trailing-slash" | "none";
    notFoundHandling?: "single-page-application" | "404-page" | "none";
    runWorkerFirst?: boolean | string[];
  };
}

/** How the node starts and where it keeps its storage. Fixed for its lifetime. */
export interface NodeOptions {
  /** Keep storage in this VirtualFileSystem. Defaults to a new in-memory one. */
  vfs?: VirtualFileSystem;
  /** Keep storage on disk in this directory instead, where the next run finds it. */
  persist?: string;
  /** Restore every cell and the deployment from a stopped node's snapshot. */
  snapshot?: CelldSnapshot;
  /** The fleet bucket's root in the VFS. Default `/celld/bucket`. */
  bucket?: string;
  /** The node's local state root in the VFS. Default `/celld/state`. */
  state?: string;
  /** The esbuild executable for Worker code. Defaults to the installed `esbuild` package. */
  esbuild?: string;
  /**
   * `CELLD_*` and `RUST_LOG` settings for the node. `RUST_LOG` defaults to
   * `warn` and `CELLD_SHUTDOWN_TOTAL_MS` to `3000`.
   */
  celldEnv?: Record<string, string>;
  /** Print the deploy report and the listener lines, as the CLI does. */
  verbose?: boolean;
}

export type CelldOptions = WorkerOptions & NodeOptions;

export interface CelldExit {
  code: number;
  message: string;
}

/** A stopped node's fleet bucket. JSON-serializable. */
export interface CelldSnapshot {
  format: "celld-snapshot-v1";
  /** The binding bridge's token, which the deployed entry module checks. */
  token: string;
  /** The deployed Wrangler config, from which the host rebuilds `env`. */
  config: Record<string, unknown>;
  /** Bucket-relative path to base64 contents. */
  files: Record<string, string>;
}

/** A Durable Object ID made on the host. It names the same object as the Worker's. */
export interface DurableObjectId {
  readonly name?: string;
  toString(): string;
  equals(other: DurableObjectId): boolean;
}

/**
 * A celld node in this process, running one Worker. A process runs one node
 * for its lifetime; `setOptions` and `redeploy` change what it runs.
 *
 * `Env` types `env` and `getBindings()`. From the host, a binding call runs
 * inside the Worker: arguments and results cross as structured-clone values
 * plus Request, Response, Headers, Blob, and R2 objects, and bodies and
 * streams cross whole. Functions and RPC stubs cannot cross yet.
 */
export class Celld<Env = Record<string, any>> implements AsyncDisposable {
  constructor(options: CelldOptions);
  /** Construct a node and wait until it serves. */
  static start<Env = Record<string, any>>(options: CelldOptions): Promise<Celld<Env>>;

  /** Settles with the node's public URL once it serves. */
  readonly ready: Promise<URL>;
  /** The node's public listener, e.g. `http://127.0.0.1:43127`. After `ready`. */
  readonly url: string;
  /** The node's internal (peer and operator) listener. After `ready`. */
  readonly internalUrl: string;
  /** Where the node's storage lives: the VFS, or `node:fs` under `persist`. */
  readonly vfs: VirtualFileSystem;
  readonly bucket: string;
  readonly state: string;
  /** Settles when the node stops. */
  readonly exited: Promise<CelldExit>;
  /** The Worker's bindings, callable from this process. Calls wait for `ready`. */
  readonly env: Env;

  /** `fetch` against the Worker. A relative `input` resolves against `url`; an absolute one keeps only its path and query. */
  fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>;
  /** Miniflare's name for `fetch`. */
  dispatchFetch(input: string | URL | Request, init?: RequestInit): Promise<Response>;

  getBindings(): Promise<Env>;
  getDurableObjectNamespace<K extends keyof Env & string>(binding: K): Promise<Env[K]>;
  getD1Database<K extends keyof Env & string>(binding: K): Promise<Env[K]>;
  getKVNamespace<K extends keyof Env & string>(binding: K): Promise<Env[K]>;
  getR2Bucket<K extends keyof Env & string>(binding: K): Promise<Env[K]>;
  getQueueProducer<K extends keyof Env & string>(binding: K): Promise<Env[K]>;

  /** Deploy `options` in place of the current Worker options, as Miniflare's `setOptions` does. Cells keep their state. */
  setOptions(options: CelldOptions): Promise<void>;
  /** Deploy the current options with `overrides` applied; `bindings` merge. With `project`, this re-reads the project. */
  redeploy(overrides?: WorkerOptions): Promise<void>;

  /** The fleet bucket of a stopped node, for `new Celld({ snapshot })` in another process. */
  snapshot(): Promise<CelldSnapshot>;

  /** Hand every cell back to the bucket and stop, as SIGTERM does for the binary. */
  stop(): Promise<CelldExit>;
  /** Miniflare's name for `stop`. */
  dispose(): Promise<void>;
  [Symbol.asyncDispose](): Promise<void>;
}

/** Start the process's celld node. Equivalent to `Celld.start(options)`. */
export function startCelld<Env = Record<string, any>>(options: CelldOptions): Promise<Celld<Env>>;

/** `node:vfs`, or Node v26.10.0's implementation where the runtime lacks it. */
export const vfs: {
  create(provider?: unknown, options?: { emitExperimentalWarning?: boolean }): VirtualFileSystem;
  MemoryProvider: new () => unknown;
  VirtualFileSystem: new (provider?: unknown, options?: object) => VirtualFileSystem;
  source: string;
};
