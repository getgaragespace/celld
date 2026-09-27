// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Values as JSON, for calls between the host process and the Worker. Both
// sides load this file: the host through require, the Worker through the
// deployment bundle. The value set is structured clone's plus the Fetch
// types (Request, Response, Headers, Blob, ReadableStream, URL) and R2
// objects, which bindings hand back. Streams and bodies travel as bytes.
"use strict";

function toBase64(bytes) {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode.apply(null, bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary);
}

function fromBase64(text) {
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
  return bytes;
}

async function readAll(stream) {
  const reader = stream.getReader();
  const chunks = [];
  let length = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    const chunk = value instanceof Uint8Array ? value : new Uint8Array(value);
    chunks.push(chunk);
    length += chunk.length;
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
}

function streamOf(bytes) {
  return new ReadableStream({
    start(controller) {
      if (bytes.length > 0) controller.enqueue(bytes);
      controller.close();
    },
  });
}

const is = (value, name) => typeof globalThis[name] === "function" && value instanceof globalThis[name];

function uncloneable(what) {
  return new TypeError(`${what} cannot cross between the host and the Worker`);
}

// An R2 object is a plain object with methods; R2's own classes are not
// exposed, so it is recognized by shape.
function isR2Object(value) {
  return typeof value.writeHttpMetadata === "function" && typeof value.key === "string" && "httpEtag" in value;
}

// R2's checksums object carries a toJSON method that renders the digests as
// hex, so it is sent as its digests and the method is rebuilt on arrival.
function checksumDigests(checksums) {
  const digests = {};
  for (const [name, digest] of Object.entries(checksums)) {
    if (typeof digest !== "function") digests[name] = digest;
  }
  return digests;
}

function checksumsOf(digests) {
  const hex = (digest) => [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return Object.defineProperty({ ...digests }, "toJSON", {
    value() {
      const json = {};
      for (const [name, digest] of Object.entries(this)) json[name] = hex(digest);
      return json;
    },
  });
}

async function encodeR2(object) {
  const fields = {};
  for (const name of ["key", "version", "size", "etag", "httpEtag", "uploaded", "httpMetadata",
    "customMetadata", "checksums", "storageClass", "range"]) {
    if (object[name] === undefined) continue;
    fields[name] = await encode(name === "checksums" ? checksumDigests(object[name]) : object[name]);
  }
  const encoded = { $: "r2", v: fields };
  if (object.body) encoded.body = toBase64(new Uint8Array(await object.arrayBuffer()));
  return encoded;
}

function decodeR2(node) {
  const object = {};
  for (const [name, value] of Object.entries(node.v)) object[name] = decode(value);
  if (object.checksums) object.checksums = checksumsOf(object.checksums);
  const http = object.httpMetadata ?? {};
  object.writeHttpMetadata = (headers) => {
    const write = (name, value) => {
      if (value !== undefined && value !== null) headers.set(name, value);
    };
    write("content-type", http.contentType);
    write("content-language", http.contentLanguage);
    write("content-disposition", http.contentDisposition);
    write("content-encoding", http.contentEncoding);
    write("cache-control", http.cacheControl);
    if (http.cacheExpiry instanceof Date) headers.set("expires", http.cacheExpiry.toUTCString());
  };
  if (node.body === undefined) return object;
  const bytes = fromBase64(node.body);
  let used = false;
  const take = () => {
    if (used) throw new TypeError("the R2 object body has already been read");
    used = true;
    return bytes;
  };
  Object.defineProperty(object, "body", { get: () => streamOf(take()), enumerable: true });
  Object.defineProperty(object, "bodyUsed", { get: () => used });
  object.arrayBuffer = async () => take().slice().buffer;
  object.bytes = async () => take().slice();
  object.text = async () => new TextDecoder().decode(take());
  object.json = async () => JSON.parse(new TextDecoder().decode(take()));
  object.blob = async () => new Blob([take()]);
  return object;
}

const NULL_BODY_STATUS = new Set([101, 103, 204, 205, 304]);

async function encode(value) {
  if (value === undefined) return { $: "u" };
  if (value === null || typeof value === "boolean" || typeof value === "string") return value;
  if (typeof value === "number") {
    if (Number.isFinite(value) && !Object.is(value, -0)) return value;
    return { $: "n", v: Object.is(value, -0) ? "-0" : String(value) };
  }
  if (typeof value === "bigint") return { $: "bi", v: value.toString() };
  if (typeof value === "function") throw uncloneable("a function");
  if (typeof value === "symbol") throw uncloneable("a symbol");
  if (Array.isArray(value)) return Promise.all(value.map(encode));
  if (value instanceof Date) return { $: "d", v: value.getTime() };
  if (value instanceof RegExp) return { $: "re", s: value.source, f: value.flags };
  if (value instanceof ArrayBuffer) return { $: "ab", v: toBase64(new Uint8Array(value)) };
  if (ArrayBuffer.isView(value)) {
    const bytes = new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    return { $: "ta", t: value.constructor.name, v: toBase64(bytes) };
  }
  if (value instanceof Map) {
    return { $: "m", v: await Promise.all([...value].map(async ([k, v]) => [await encode(k), await encode(v)])) };
  }
  if (value instanceof Set) return { $: "s", v: await Promise.all([...value].map(encode)) };
  if (value instanceof Error) {
    const encoded = { $: "e", name: value.name, message: value.message, stack: value.stack, code: value.code };
    if (value.cause !== undefined) encoded.cause = await encode(value.cause).catch(() => undefined);
    return encoded;
  }
  if (is(value, "Headers")) return { $: "h", v: [...value] };
  if (is(value, "URL")) return { $: "url", v: value.href };
  if (is(value, "Request")) {
    const body = value.body ? toBase64(new Uint8Array(await value.arrayBuffer())) : null;
    return { $: "req", url: value.url, method: value.method, headers: [...value.headers], body };
  }
  if (is(value, "Response")) {
    const body = value.body ? toBase64(new Uint8Array(await value.arrayBuffer())) : null;
    return { $: "res", status: value.status, statusText: value.statusText, headers: [...value.headers], body };
  }
  if (is(value, "Blob")) return { $: "blob", type: value.type, v: toBase64(new Uint8Array(await value.arrayBuffer())) };
  if (is(value, "ReadableStream")) return { $: "rs", v: toBase64(await readAll(value)) };
  if (isR2Object(value)) return encodeR2(value);
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    throw uncloneable(`a ${prototype?.constructor?.name ?? "class"} instance`);
  }
  const fields = {};
  for (const [key, field] of Object.entries(value)) {
    if (typeof field === "function") throw uncloneable(`the function at "${key}"`);
    fields[key] = await encode(field);
  }
  return { $: "o", v: fields };
}

function decode(node) {
  if (node === null || typeof node !== "object") return node;
  if (Array.isArray(node)) return node.map(decode);
  switch (node.$) {
    case "u": return undefined;
    case "n": return node.v === "-0" ? -0 : Number(node.v);
    case "bi": return BigInt(node.v);
    case "d": return new Date(node.v);
    case "re": return new RegExp(node.s, node.f);
    case "ab": return fromBase64(node.v).buffer;
    case "ta": {
      const bytes = fromBase64(node.v);
      if (node.t === "Uint8Array") return bytes;
      if (node.t === "DataView") return new DataView(bytes.buffer);
      const View = globalThis[node.t];
      return new View(bytes.buffer, 0, bytes.byteLength / View.BYTES_PER_ELEMENT);
    }
    case "m": return new Map(node.v.map(([k, v]) => [decode(k), decode(v)]));
    case "s": return new Set(node.v.map(decode));
    case "e": {
      const Kind = globalThis[node.name];
      const known = typeof Kind === "function" && (Kind === Error || Kind.prototype instanceof Error);
      // AggregateError takes its errors first.
      const error = !known ? new Error(node.message)
        : Kind === globalThis.AggregateError ? new Kind([], node.message)
        : new Kind(node.message);
      error.name = node.name;
      if (node.stack) error.stack = node.stack;
      if (node.code !== undefined) error.code = node.code;
      if (node.cause !== undefined) error.cause = decode(node.cause);
      return error;
    }
    case "h": return new Headers(node.v);
    case "url": return new URL(node.v);
    case "req": {
      const body = node.body === null ? null : fromBase64(node.body);
      return new Request(node.url, { method: node.method, headers: node.headers, body });
    }
    case "res": {
      const body = node.body === null || NULL_BODY_STATUS.has(node.status) ? null : fromBase64(node.body);
      return new Response(body, { status: node.status, statusText: node.statusText, headers: node.headers });
    }
    case "blob": return new Blob([fromBase64(node.v)], { type: node.type });
    case "rs": return streamOf(fromBase64(node.v));
    case "r2": return decodeR2(node);
    case "o": {
      const value = {};
      for (const [key, field] of Object.entries(node.v)) value[key] = decode(field);
      return value;
    }
    default: throw new TypeError(`unknown encoded value: ${node.$}`);
  }
}

module.exports = { encode, decode };
