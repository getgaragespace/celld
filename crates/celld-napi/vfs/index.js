// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// `node:vfs` for runtimes that do not ship it.
//
// Node v26 has `node:vfs` behind --experimental-vfs; Bun does not have it.
// When the runtime provides the module, this file returns it. Otherwise it
// evaluates the unmodified Node v26.10.0 sources in ./node (MIT, see
// ./node/LICENSE) with the handful of Node internals they use supplied below.
// Mounting over `fs` (lib/internal/vfs/setup.js) and the zip, real-filesystem
// and single-executable providers are not carried; they throw when used.
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

function builtin() {
  try {
    return process.getBuiltinModule?.("node:vfs") ?? null;
  } catch {
    return null;
  }
}

const SOURCE = path.join(__dirname, "node", "lib");

function uncurryThis(fn) {
  return (self, ...args) => Reflect.apply(fn, self, args);
}

const GLOBALS = [
  "BigInt64Array", "Float64Array", "BigInt", "Array", "Date", "Error",
  "Function", "Math", "Number", "Object", "Promise", "String", "Symbol",
];

// Node's primordials, derived from their names: `ArrayPrototypePush` is an
// uncurried Array.prototype.push, `FunctionPrototypeSymbolHasInstance` the
// uncurried Function.prototype[Symbol.hasInstance], `ArrayFrom` is Array.from.
const primordials = new Proxy(Object.create(null), {
  get(cache, name) {
    if (typeof name !== "string") return undefined;
    if (name in cache) return cache[name];
    let value;
    if (name.startsWith("Safe")) {
      value = globalThis[name.slice(4)];
    } else {
      const owner = GLOBALS.find((global) => name.startsWith(global));
      if (!owner) throw new Error(`vfs shim: unknown primordial ${name}`);
      const target = globalThis[owner];
      const rest = name.slice(owner.length);
      if (rest === "") {
        value = target;
      } else if (rest.startsWith("Prototype")) {
        const member = rest.slice("Prototype".length);
        const key = member.startsWith("Symbol")
          ? Symbol[member[6].toLowerCase() + member.slice(7)]
          : member[0].toLowerCase() + member.slice(1);
        value = uncurryThis(target.prototype[key]);
      } else {
        const key = rest[0].toLowerCase() + rest.slice(1);
        value = typeof target[key] === "function" ? target[key].bind(target) : target[key];
      }
    }
    if (value === undefined) throw new Error(`vfs shim: unresolved primordial ${name}`);
    return (cache[name] = value);
  },
});

function codedError(code, Base = Error) {
  return class extends Base {
    constructor(...args) {
      super(args.map(String).join(" ") || code);
      this.code = code;
    }
  };
}

const errorCodes = new Proxy(Object.create(null), {
  get(cache, code) {
    if (typeof code !== "string") return undefined;
    return (cache[code] ??= codedError(code));
  },
});

const uv = Object.fromEntries(
  Object.entries(os.constants.errno).map(([name, value]) => [`UV_${name}`, -value]),
);

class UVException extends Error {
  constructor(context) {
    const name = Object.keys(os.constants.errno).find(
      (key) => -os.constants.errno[key] === context.errno,
    ) ?? context.code ?? "UNKNOWN";
    let message = `${name}: ${context.message ?? name.toLowerCase()}, ${context.syscall}`;
    if (context.path) message += ` '${context.path}'`;
    if (context.dest) message += ` -> '${context.dest}'`;
    super(message);
    this.errno = context.errno;
    this.code = name;
    this.syscall = context.syscall;
    if (context.path) this.path = context.path;
    if (context.dest) this.dest = context.dest;
  }
}

class AbortError extends Error {
  constructor(message = "The operation was aborted", options = undefined) {
    super(message, options);
    this.code = "ABORT_ERR";
    this.name = "AbortError";
  }
}

const { S_IFMT, S_IFREG, S_IFDIR, S_IFLNK, S_IFBLK, S_IFCHR, S_IFIFO, S_IFSOCK } = fs.constants;

class Stats {
  constructor(fields, bigint) {
    const [dev, mode, nlink, uid, gid, rdev, blksize, ino, size, blocks] = fields;
    Object.assign(this, { dev, mode, nlink, uid, gid, rdev, blksize, ino, size, blocks });
    const times = ["atime", "mtime", "ctime", "birthtime"];
    times.forEach((name, index) => {
      const sec = fields[10 + index * 2];
      const nsec = fields[11 + index * 2];
      if (bigint) {
        this[`${name}Ms`] = sec * 1000n + nsec / 1000000n;
        this[`${name}Ns`] = sec * 1000000000n + nsec;
        this[name] = new Date(Number(this[`${name}Ms`]));
      } else {
        this[`${name}Ms`] = sec * 1000 + nsec / 1e6;
        this[name] = new Date(this[`${name}Ms`]);
      }
    });
  }
  #type() {
    return Number(this.mode) & S_IFMT;
  }
  isFile() { return this.#type() === S_IFREG; }
  isDirectory() { return this.#type() === S_IFDIR; }
  isSymbolicLink() { return this.#type() === S_IFLNK; }
  isBlockDevice() { return this.#type() === S_IFBLK; }
  isCharacterDevice() { return this.#type() === S_IFCHR; }
  isFIFO() { return this.#type() === S_IFIFO; }
  isSocket() { return this.#type() === S_IFSOCK; }
}

const FLAGS = (() => {
  const { O_RDONLY, O_RDWR, O_WRONLY, O_CREAT, O_TRUNC, O_APPEND, O_EXCL, O_SYNC } = fs.constants;
  return {
    r: O_RDONLY, rs: O_RDONLY | O_SYNC, sr: O_RDONLY | O_SYNC,
    "r+": O_RDWR, "rs+": O_RDWR | O_SYNC, "sr+": O_RDWR | O_SYNC,
    w: O_TRUNC | O_CREAT | O_WRONLY, wx: O_TRUNC | O_CREAT | O_WRONLY | O_EXCL,
    xw: O_TRUNC | O_CREAT | O_WRONLY | O_EXCL,
    "w+": O_TRUNC | O_CREAT | O_RDWR, "wx+": O_TRUNC | O_CREAT | O_RDWR | O_EXCL,
    "xw+": O_TRUNC | O_CREAT | O_RDWR | O_EXCL,
    a: O_APPEND | O_CREAT | O_WRONLY, ax: O_APPEND | O_CREAT | O_WRONLY | O_EXCL,
    xa: O_APPEND | O_CREAT | O_WRONLY | O_EXCL, as: O_APPEND | O_CREAT | O_WRONLY | O_SYNC,
    sa: O_APPEND | O_CREAT | O_WRONLY | O_SYNC,
    "a+": O_APPEND | O_CREAT | O_RDWR, "ax+": O_APPEND | O_CREAT | O_RDWR | O_EXCL,
    "xa+": O_APPEND | O_CREAT | O_RDWR | O_EXCL, "as+": O_APPEND | O_CREAT | O_RDWR | O_SYNC,
    "sa+": O_APPEND | O_CREAT | O_RDWR | O_SYNC,
  };
})();

function invalidArgument(name, expected, value) {
  const error = new TypeError(`The "${name}" argument must be ${expected}. Received ${typeof value}`);
  error.code = "ERR_INVALID_ARG_TYPE";
  return error;
}

const unsupported = (what) => () => {
  throw new Error(`${what} is not available in the vendored node:vfs`);
};

const shims = {
  "internal/errors": { codes: errorCodes, UVException, AbortError },
  "internal/util": {
    kEmptyObject: Object.freeze({ __proto__: null }),
    emitExperimentalWarning() {},
  },
  "internal/util/debuglog": { debuglog: () => () => {} },
  "internal/url": { pathToFileURL: require("node:url").pathToFileURL },
  "internal/validators": {
    validateBoolean(value, name) {
      if (typeof value !== "boolean") throw invalidArgument(name, "of type boolean", value);
    },
    validateString(value, name) {
      if (typeof value !== "string") throw invalidArgument(name, "of type string", value);
    },
    validateFunction(value, name) {
      if (typeof value !== "function") throw invalidArgument(name, "of type function", value);
    },
    validateObject(value, name) {
      if (value === null || typeof value !== "object") throw invalidArgument(name, "of type object", value);
    },
    validateInteger(value, name, min = Number.MIN_SAFE_INTEGER, max = Number.MAX_SAFE_INTEGER) {
      if (!Number.isInteger(value) || value < min || value > max) {
        const error = new RangeError(`The value of "${name}" is out of range. Received ${value}`);
        error.code = "ERR_OUT_OF_RANGE";
        throw error;
      }
    },
    parseFileMode(value, name, fallback) {
      value ??= fallback;
      if (typeof value === "string") value = Number.parseInt(value, 8);
      if (!Number.isInteger(value) || value < 0 || value > 0o7777777) {
        throw invalidArgument(name, "a 32-bit unsigned integer or an octal string", value);
      }
      return value;
    },
  },
  "internal/fs/utils": {
    Dirent: fs.Dirent,
    getStatsFromBinding(fields, offset = 0) {
      const slice = Array.from(fields.subarray(offset, offset + 18));
      return new Stats(slice, typeof slice[0] === "bigint");
    },
    stringToFlags(flags, name = "flags") {
      if (typeof flags === "number") return flags;
      if (flags == null) return fs.constants.O_RDONLY;
      if (flags in FLAGS) return FLAGS[flags];
      const error = new TypeError(`The value "${flags}" is invalid for option "${name}"`);
      error.code = "ERR_INVALID_ARG_VALUE";
      throw error;
    },
    toUnixTimestamp(time, name = "time") {
      if (typeof time === "number") return time;
      if (typeof time === "string" && time !== "" && Number.isFinite(Number(time))) return Number(time);
      if (time instanceof Date) return time.getTime() / 1000;
      throw invalidArgument(name, "of type number, string or Date", time);
    },
  },
  "internal/vfs/setup": {
    registerVFS: unsupported("mounting a VirtualFileSystem"),
    deregisterVFS: unsupported("unmounting a VirtualFileSystem"),
  },
  "internal/vfs/providers/real": { RealFSProvider: unsupported("RealFSProvider") },
  "internal/vfs/providers/ziparchive": { ZipProvider: unsupported("ZipProvider") },
  "internal/vfs/sea": {},
  "internal/zip": { ZipFile: unsupported("ZipFile") },
};

function internalBinding(name) {
  if (name === "constants") return { fs: fs.constants };
  if (name === "uv") return uv;
  throw new Error(`vfs shim: unknown internal binding ${name}`);
}

function vendored() {
  const modules = new Map();
  function load(id) {
    if (id in shims) return shims[id];
    if (!id.startsWith("internal/") && id !== "vfs") return require(`node:${id}`);
    const cached = modules.get(id);
    if (cached) return cached.exports;
    const file = path.join(SOURCE, `${id}.js`);
    const module = { exports: {} };
    modules.set(id, module);
    const body = `${fs.readFileSync(file, "utf8")}\n//# sourceURL=${file}`;
    const evaluate = new Function("exports", "require", "module", "primordials", "internalBinding", "process", body);
    evaluate(module.exports, load, module, primordials, internalBinding, process);
    return module.exports;
  }
  return load("vfs");
}

const native = builtin();

module.exports = {
  ...(native ?? vendored()),
  source: native ? "node" : "vendored node v26.10.0",
};
