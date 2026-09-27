// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// The Worker half of the host's `env` proxies. The SDK deploys the user's
// Worker behind a generated entry module that wraps its default export with
// `withBridge`. The wrapper answers one POST route, authenticated by a
// per-node token, by running a binding call against the Worker's real `env`;
// every other request reaches the user's handler unchanged.
import codec from "./codec.js";

const { encode, decode } = codec;

export const BRIDGE_PATH = "/__celld_sdk__";
export const BRIDGE_HEADER = "x-celld-sdk";

function statement(db, { sql, binds }) {
  const prepared = db.prepare(sql);
  return binds.length > 0 ? prepared.bind(...binds) : prepared;
}

function runD1(db, op) {
  switch (op.action) {
    case "exec": return db.exec(op.sql);
    case "batch": return db.batch(op.statements.map((each) => statement(db, each)));
    case "first": {
      const prepared = statement(db, op);
      return op.column === undefined ? prepared.first() : prepared.first(op.column);
    }
    case "raw": {
      const prepared = statement(db, op);
      return op.options === undefined ? prepared.raw() : prepared.raw(op.options);
    }
    case "all": return statement(db, op).all();
    case "run": return statement(db, op).run();
    default: throw new TypeError(`unknown D1 action: ${op.action}`);
  }
}

function run(op, env) {
  const binding = env[op.binding];
  if (binding === undefined || binding === null) {
    throw new ReferenceError(`the Worker's env has no binding named ${op.binding}`);
  }
  if (op.kind === "d1") return runD1(binding, op);
  let target = binding;
  if (op.id) {
    const id = op.id.name !== undefined ? binding.idFromName(op.id.name) : binding.idFromString(op.id.hex);
    target = binding.get(id);
  }
  const method = target[op.method];
  if (typeof method !== "function") {
    throw new TypeError(`${op.binding}${op.id ? "'s stub" : ""} has no method ${op.method}`);
  }
  // An RPC method is a proxy that answers every property, `apply` included.
  return Reflect.apply(method, target, op.args);
}

async function answer(request, env, token) {
  if (request.method !== "POST" || request.headers.get(BRIDGE_HEADER) !== token) return null;
  if (new URL(request.url).pathname !== BRIDGE_PATH) return null;
  try {
    const value = await run(decode(await request.json()), env);
    return Response.json({ ok: true, value: await encode(value) });
  } catch (error) {
    const thrown = error instanceof Error ? error : new Error(String(error));
    return Response.json({ ok: false, error: await encode(thrown) });
  }
}

const notFound = () => new Response("Not Found", { status: 404 });

export function withBridge(inner, token) {
  if (typeof inner === "function") {
    return class extends inner {
      async fetch(request) {
        return (await answer(request, this.env, token)) ??
          (super.fetch ? super.fetch(request) : notFound());
      }
    };
  }
  const handlers = inner ?? {};
  return {
    ...handlers,
    async fetch(request, env, ctx) {
      return (await answer(request, env, token)) ??
        (handlers.fetch ? handlers.fetch.call(handlers, request, env, ctx) : notFound());
    },
  };
}
