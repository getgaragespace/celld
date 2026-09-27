// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// The host's view of the Worker's `env`. Vars are the config's strings; every
// other binding is a proxy whose calls run inside the Worker, through the
// bridge route the generated entry module answers (lib/bridge.mjs).
"use strict";

const crypto = require("node:crypto");

/**
 * Durable Object IDs, derived the way the node derives them so that an ID
 * made here names the same object as one made in the Worker.
 */
function objectIds(namespaceKey) {
  const key = crypto.createHash("sha256").update(namespaceKey).digest();
  const mac = (data) => crypto.createHmac("sha256", key).update(data).digest().subarray(0, 16);
  const withTail = (head) => Buffer.concat([head, mac(head)]).toString("hex");
  return {
    fromName: (name) => withTail(mac(Buffer.from(name, "utf8"))),
    unique: () => withTail(crypto.randomBytes(16)),
    validate(value) {
      if (!/^[0-9a-f]{64}$/i.test(value)) {
        throw new TypeError("Invalid Durable Object ID: must be 64 hex digits");
      }
      const hex = value.toLowerCase();
      const head = Buffer.from(hex.slice(0, 32), "hex");
      if (!mac(head).equals(Buffer.from(hex.slice(32), "hex"))) {
        throw new TypeError("Durable Object ID is not valid for this namespace");
      }
      return hex;
    },
  };
}

const namespaceKey = (scriptName, className) => `cells:v1:${scriptName.length}:${scriptName}:${className}`;

class DurableObjectId {
  #hex;
  #namespace;
  constructor(namespace, hex, name) {
    this.#namespace = namespace;
    this.#hex = hex;
    if (name !== undefined) this.name = name;
  }
  static namespaceOf(id) {
    return #namespace in id ? id.#namespace : undefined;
  }
  toString() {
    return this.#hex;
  }
  toJSON() {
    return this.#hex;
  }
  equals(other) {
    return other instanceof DurableObjectId && other.#hex === this.#hex;
  }
}

function toRequest(input, init) {
  return input instanceof Request && init === undefined ? input : new Request(input, init);
}

function stubOf(call, binding, id) {
  const wire = id.name !== undefined ? { name: id.name, hex: String(id) } : { hex: String(id) };
  const invoke = (method, args) => call({ kind: "call", binding, id: wire, method, args });
  const target = {
    id,
    name: id.name,
    fetch: (input, init) => invoke("fetch", [toRequest(input, init)]),
  };
  return new Proxy(target, {
    get(stub, property) {
      if (Reflect.has(stub, property)) return Reflect.get(stub, property);
      if (property === "then" || typeof property !== "string") return undefined;
      return (...args) => invoke(property, args);
    },
  });
}

class DurableObjectNamespace {
  #call;
  #binding;
  #ids;
  constructor(call, binding, key) {
    this.#call = call;
    this.#binding = binding;
    this.#ids = objectIds(key);
  }
  idFromName(name) {
    name = String(name);
    return new DurableObjectId(this, this.#ids.fromName(name), name);
  }
  idFromString(value) {
    return new DurableObjectId(this, this.#ids.validate(String(value)));
  }
  newUniqueId(options) {
    if (options?.jurisdiction != null) throw new Error("Jurisdiction restrictions are not implemented");
    return new DurableObjectId(this, this.#ids.unique());
  }
  jurisdiction(value) {
    if (value == null) return this;
    throw new Error("Jurisdiction restrictions are not implemented");
  }
  get(id) {
    if (!(id instanceof DurableObjectId) || DurableObjectId.namespaceOf(id) !== this) {
      throw new TypeError("Durable Object ID is not valid for this namespace");
    }
    return stubOf(this.#call, this.#binding, id);
  }
  getByName(name) {
    return this.get(this.idFromName(name));
  }
}

const STATEMENT = Symbol("statement");

class D1PreparedStatement {
  #run;
  #sql;
  #binds;
  constructor(run, sql, binds) {
    this.#run = run;
    this.#sql = sql;
    this.#binds = binds;
  }
  get [STATEMENT]() {
    return { sql: this.#sql, binds: this.#binds };
  }
  bind(...values) {
    return new D1PreparedStatement(this.#run, this.#sql, values);
  }
  first(column) {
    return this.#run({ action: "first", sql: this.#sql, binds: this.#binds, column });
  }
  all() {
    return this.#run({ action: "all", sql: this.#sql, binds: this.#binds });
  }
  run() {
    return this.#run({ action: "run", sql: this.#sql, binds: this.#binds });
  }
  raw(options) {
    return this.#run({ action: "raw", sql: this.#sql, binds: this.#binds, options });
  }
}

class D1Database {
  #run;
  constructor(call, binding) {
    this.#run = (op) => call({ kind: "d1", binding, ...op });
  }
  prepare(sql) {
    return new D1PreparedStatement(this.#run, String(sql), []);
  }
  batch(statements) {
    return this.#run({ action: "batch", statements: statements.map((statement) => statement[STATEMENT]) });
  }
  exec(sql) {
    return this.#run({ action: "exec", sql: String(sql) });
  }
  withSession() {
    throw new Error("D1 sessions are not available from the host yet");
  }
  dump() {
    throw new Error("D1 dump is not available from the host");
  }
}

function methods(call, binding, names) {
  const object = {};
  for (const method of names) object[method] = (...args) => call({ kind: "call", binding, method, args });
  return Object.freeze(object);
}

const KV_METHODS = ["get", "getWithMetadata", "put", "delete", "list"];
const R2_METHODS = ["head", "get", "put", "delete", "list"];
const QUEUE_METHODS = ["send", "sendBatch", "metrics"];

/** A service binding: `fetch` plus RPC to any other method of the entrypoint. */
function fetcher(call, binding) {
  const invoke = (method, args) => call({ kind: "call", binding, method, args });
  const target = { fetch: (input, init) => invoke("fetch", [toRequest(input, init)]) };
  return new Proxy(target, {
    get(service, property) {
      if (Reflect.has(service, property)) return Reflect.get(service, property);
      if (property === "then" || typeof property !== "string") return undefined;
      return (...args) => invoke(property, args);
    },
  });
}

function unavailable(binding, kind) {
  return new Proxy({}, {
    get(_, property) {
      if (property === "then" || typeof property !== "string") return undefined;
      throw new Error(`${binding} is a ${kind} binding, which is not available from the host yet`);
    },
  });
}

/** The host's `env` for the deployed `config`, with calls sent by `call`. */
function createEnv(config, call) {
  const env = {};
  for (const [name, value] of Object.entries(config.vars ?? {})) env[name] = value;
  for (const { name, class_name } of config.durable_objects?.bindings ?? []) {
    env[name] = new DurableObjectNamespace(call, name, namespaceKey(config.name, class_name));
  }
  for (const { binding } of config.d1_databases ?? []) env[binding] = new D1Database(call, binding);
  for (const { binding } of config.kv_namespaces ?? []) env[binding] = methods(call, binding, KV_METHODS);
  for (const { binding } of config.r2_buckets ?? []) env[binding] = methods(call, binding, R2_METHODS);
  for (const { binding } of config.queues?.producers ?? []) env[binding] = methods(call, binding, QUEUE_METHODS);
  for (const { binding } of config.services ?? []) env[binding] = fetcher(call, binding);
  if (config.assets?.binding) env[config.assets.binding] = fetcher(call, config.assets.binding);
  for (const { binding } of config.workflows ?? []) env[binding] = unavailable(binding, "Workflow");
  return env;
}

module.exports = { createEnv, DurableObjectId, DurableObjectNamespace, D1Database, D1PreparedStatement };
