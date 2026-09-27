// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Options to a deployable project. Miniflare-style options become the
// Wrangler config `celld deploy` reads; a Wrangler project is read as is and
// the options' bindings are merged into it. Either way the SDK deploys a
// generated project in a temporary directory whose entry module wraps the
// Worker's default export with the binding bridge (lib/bridge.mjs).
"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const BRIDGE_MODULE = path.join(__dirname, "bridge.mjs");

// Copy `source` a character at a time, handing every character outside a
// string literal to `visit`, which returns what to emit and how far to skip.
function outsideStrings(source, visit) {
  let out = "";
  let index = 0;
  while (index < source.length) {
    if (source[index] === '"') {
      const start = index++;
      while (index < source.length && source[index] !== '"') index += source[index] === "\\" ? 2 : 1;
      out += source.slice(start, ++index);
      continue;
    }
    const [emit, next] = visit(index);
    out += emit;
    index = next;
  }
  return out;
}

/** JSON with comments and trailing commas, as Wrangler reads it. */
function parseJsonc(source) {
  const bare = outsideStrings(source, (index) => {
    if (source.startsWith("//", index)) {
      const end = source.indexOf("\n", index);
      return ["", end === -1 ? source.length : end];
    }
    if (source.startsWith("/*", index)) {
      const end = source.indexOf("*/", index + 2);
      return [" ", end === -1 ? source.length : end + 2];
    }
    return [source[index], index + 1];
  });
  const json = outsideStrings(bare, (index) => {
    if (bare[index] !== ",") return [bare[index], index + 1];
    let next = index + 1;
    while (next < bare.length && /\s/.test(bare[next])) next++;
    return [bare[next] === "}" || bare[next] === "]" ? "" : ",", index + 1];
  });
  return JSON.parse(json);
}

function resolveProjectConfig(project) {
  const given = path.resolve(project);
  if (!fs.statSync(given).isDirectory()) return given;
  for (const candidate of ["wrangler.jsonc", "wrangler.json"]) {
    const file = path.join(given, candidate);
    if (fs.existsSync(file)) return file;
  }
  if (fs.existsSync(path.join(given, "wrangler.toml"))) {
    throw new Error("wrangler.toml is not supported; convert it to wrangler.jsonc");
  }
  throw new Error(`no wrangler.jsonc or wrangler.json in ${given}`);
}

const entries = (value, what) => {
  if (value === undefined) return [];
  if (Array.isArray(value)) return value.map((name) => [name, name]);
  if (typeof value === "object" && value !== null) return Object.entries(value);
  throw new TypeError(`${what} must be an array or an object`);
};

const defined = (object) => Object.fromEntries(Object.entries(object).filter(([, value]) => value !== undefined));

/** The Wrangler config fragment that Miniflare-style binding options describe. */
function bindingConfig(options, scriptName) {
  const config = {};
  const vars = {};
  for (const [name, value] of Object.entries(options.bindings ?? {})) {
    if (typeof value !== "string") {
      throw new TypeError(`bindings.${name} must be a string: celld's vars are plain text`);
    }
    vars[name] = value;
  }
  if (Object.keys(vars).length > 0) config.vars = vars;

  const doBindings = [];
  for (const [name, value] of entries(options.durableObjects, "durableObjects")) {
    const spec = typeof value === "string" ? { className: value } : value;
    if (spec.scriptName !== undefined && spec.scriptName !== scriptName) {
      throw new Error(`durableObjects.${name}: celld binds only this Worker's classes, not ${spec.scriptName}'s`);
    }
    doBindings.push({ name, class_name: spec.className });
  }
  if (doBindings.length > 0) config.durable_objects = { bindings: doBindings };

  const d1 = entries(options.d1Databases, "d1Databases").map(([binding, id]) => ({ binding, database_name: id }));
  if (d1.length > 0) config.d1_databases = d1;
  const kv = entries(options.kvNamespaces, "kvNamespaces").map(([binding, id]) => ({ binding, id }));
  if (kv.length > 0) config.kv_namespaces = kv;
  const r2 = entries(options.r2Buckets, "r2Buckets").map(([binding, bucket]) => ({ binding, bucket_name: bucket }));
  if (r2.length > 0) config.r2_buckets = r2;

  const producers = entries(options.queueProducers, "queueProducers").map(([binding, value]) => {
    const spec = typeof value === "string" ? { queueName: value } : value;
    return defined({ binding, queue: spec.queueName, delivery_delay: spec.deliveryDelay });
  });
  const consumers = entries(options.queueConsumers, "queueConsumers").map(([queue, value]) => {
    const spec = typeof value === "string" ? {} : value;
    return defined({
      queue,
      max_batch_size: spec.maxBatchSize,
      max_batch_timeout: spec.maxBatchTimeout,
      max_retries: spec.maxRetries,
      dead_letter_queue: spec.deadLetterQueue,
      max_concurrency: spec.maxConcurrency,
      retry_delay: spec.retryDelay,
    });
  });
  if (producers.length > 0 || consumers.length > 0) {
    config.queues = defined({
      producers: producers.length > 0 ? producers : undefined,
      consumers: consumers.length > 0 ? consumers : undefined,
    });
  }

  const services = [];
  for (const [binding, value] of Object.entries(options.serviceBindings ?? {})) {
    if (typeof value === "function") {
      throw new Error(`serviceBindings.${binding}: a host function as a service is not supported yet`);
    }
    const spec = typeof value === "string" ? { name: value } : value;
    if (typeof spec.name !== "string") {
      throw new Error(`serviceBindings.${binding}: celld binds services by Worker name only`);
    }
    services.push(defined({ binding, service: spec.name, entrypoint: spec.entrypoint }));
  }
  if (services.length > 0) config.services = services;

  const workflows = Object.entries(options.workflows ?? {}).map(([binding, spec]) => {
    if (spec.scriptName !== undefined && spec.scriptName !== scriptName) {
      throw new Error(`workflows.${binding}: celld binds only this Worker's workflows`);
    }
    return { binding, name: spec.name, class_name: spec.className };
  });
  if (workflows.length > 0) config.workflows = workflows;

  if (options.crons !== undefined) config.triggers = { crons: [...options.crons] };
  return config;
}

function sqliteClasses(config) {
  return new Set((config.migrations ?? []).flatMap((migration) => migration.new_sqlite_classes ?? []));
}

/** `extra` merged into `base`: arrays concatenate and `vars` merge. */
function mergeConfig(base, extra) {
  const merged = { ...base };
  if (extra.vars) merged.vars = { ...base.vars, ...extra.vars };
  for (const key of ["d1_databases", "kv_namespaces", "r2_buckets", "services", "workflows"]) {
    if (extra[key]) merged[key] = [...(base[key] ?? []), ...extra[key]];
  }
  if (extra.durable_objects) {
    const bindings = [...(base.durable_objects?.bindings ?? []), ...extra.durable_objects.bindings];
    merged.durable_objects = { ...base.durable_objects, bindings };
    const declared = sqliteClasses(base);
    const added = [...new Set(extra.durable_objects.bindings.map((binding) => binding.class_name))]
      .filter((name) => !declared.has(name));
    if (added.length > 0) {
      merged.migrations = [...(base.migrations ?? []), { tag: "celld-sdk", new_sqlite_classes: added }];
    }
  }
  if (extra.queues) {
    merged.queues = { ...base.queues };
    for (const key of ["producers", "consumers"]) {
      if (extra.queues[key]) merged.queues[key] = [...(base.queues?.[key] ?? []), ...extra.queues[key]];
    }
  }
  if (extra.triggers) {
    merged.triggers = { ...base.triggers, crons: [...(base.triggers?.crons ?? []), ...extra.triggers.crons] };
  }
  return merged;
}

function wrapper(main, token) {
  const user = JSON.stringify(main);
  return [
    `import * as user from ${user};`,
    `import { withBridge } from ${JSON.stringify(BRIDGE_MODULE)};`,
    `export * from ${user};`,
    `export default withBridge(user.default, ${JSON.stringify(token)});`,
    "",
  ].join("\n");
}

/**
 * Write the project for `options` and return its config. `dispose` removes
 * what was written.
 */
function prepareProject(options, token) {
  const sources = ["script", "scriptPath", "project"].filter((key) => options[key] !== undefined);
  if (sources.length !== 1) {
    throw new TypeError("give the Worker as exactly one of `script`, `scriptPath`, or `project`");
  }
  if (options.modules === false) throw new Error("celld runs ES module Workers only; service-worker syntax is not supported");

  let base;
  let root = null;
  let main;
  if (options.project !== undefined) {
    const configPath = resolveProjectConfig(options.project);
    root = path.dirname(configPath);
    base = parseJsonc(fs.readFileSync(configPath, "utf8"));
    if (base.no_bundle) {
      throw new Error("a `no_bundle` project cannot carry the SDK's binding bridge; deploy it with the celld CLI");
    }
    if (base.containers) throw new Error("the SDK does not run containers yet");
    if (typeof base.main !== "string") throw new Error(`${configPath} has no \`main\``);
    main = path.resolve(root, base.main);
  } else {
    base = { name: "worker" };
  }
  if (options.name !== undefined) base.name = options.name;
  if (options.compatibilityDate !== undefined) base.compatibility_date = options.compatibilityDate;
  if (options.compatibilityFlags !== undefined) base.compatibility_flags = [...options.compatibilityFlags];
  const config = mergeConfig(base, bindingConfig(options, base.name));

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "celld-sdk-"));
  try {
    if (options.script !== undefined) {
      main = path.join(dir, "worker.mjs");
      fs.writeFileSync(main, options.script);
    } else if (options.scriptPath !== undefined) {
      main = path.resolve(options.scriptPath);
    }
    fs.writeFileSync(path.join(dir, "entry.mjs"), wrapper(main, token));
    config.main = "entry.mjs";

    const assets = options.assets ?? config.assets;
    if (assets) {
      const from = path.resolve(options.assets ? process.cwd() : root, assets.directory);
      fs.cpSync(from, path.join(dir, "assets"), { recursive: true });
      config.assets = options.assets
        ? defined({
          directory: "assets",
          binding: assets.binding,
          html_handling: assets.htmlHandling,
          not_found_handling: assets.notFoundHandling,
          run_worker_first: assets.runWorkerFirst,
        })
        : { ...assets, directory: "assets" };
    }

    const configPath = path.join(dir, "wrangler.json");
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
    return {
      config,
      configPath,
      dispose: () => fs.rmSync(dir, { recursive: true, force: true }),
    };
  } catch (error) {
    fs.rmSync(dir, { recursive: true, force: true });
    throw error;
  }
}

module.exports = { prepareProject, parseJsonc, bindingConfig, mergeConfig };
