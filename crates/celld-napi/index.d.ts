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
  readdirSync(path: string, options: { withFileTypes: true }): Array<{ name: string; isDirectory(): boolean }>;
  mkdirSync(path: string, options: { recursive: true }): unknown;
  renameSync(from: string, to: string): void;
  unlinkSync(path: string): void;
  rmSync(path: string, options: { recursive: true; force: true }): void;
  existsSync(path: string): boolean;
}

export interface CelldOptions {
  /** Where the node's storage lives. Defaults to a new in-memory VirtualFileSystem. */
  vfs?: VirtualFileSystem;
  /** Deploy this Wrangler project before the node starts, as `celld deploy` does. */
  project?: string;
  /** The fleet bucket's root in the VFS. Default `/celld/bucket`. */
  bucket?: string;
  /** The node's local state root in the VFS. Default `/celld/state`. */
  state?: string;
  /** The esbuild executable for Worker code. Defaults to the installed `esbuild` package. */
  esbuild?: string;
  /**
   * `CELLD_*` and `RUST_LOG` settings for the node. `RUST_LOG` defaults to
   * `warn` and `CELLD_SHUTDOWN_TOTAL_MS` to `3000`: with no peer to adopt its
   * cells, a stopping node would otherwise wait out celld's full drain budget.
   */
  env?: Record<string, string>;
}

export interface CelldExit {
  code: number;
  message: string;
}

export interface Celld {
  /** The node's public listener, e.g. `http://127.0.0.1:43127`. */
  readonly url: string;
  /** The node's internal (peer and operator) listener. */
  readonly internalUrl: string;
  readonly vfs: VirtualFileSystem;
  readonly bucket: string;
  readonly state: string;
  /** Settles when the node stops. */
  readonly exited: Promise<CelldExit>;
  /** `fetch` against the node, with `input` resolved relative to `url`. */
  fetch(input: string | URL, init?: RequestInit): Promise<Response>;
  /** Hand every cell back to the bucket and stop, as SIGTERM does for the binary. */
  stop(): Promise<CelldExit>;
}

/** Start the process's celld node. A process runs at most one. */
export function startCelld(options?: CelldOptions): Promise<Celld>;

/** `node:vfs`, or Node v26.10.0's implementation where the runtime lacks it. */
export const vfs: {
  create(provider?: unknown, options?: { emitExperimentalWarning?: boolean }): VirtualFileSystem;
  MemoryProvider: new () => unknown;
  VirtualFileSystem: new (provider?: unknown, options?: object) => VirtualFileSystem;
  source: string;
};
