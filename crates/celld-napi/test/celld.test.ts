import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Celld, vfs, type VirtualFileSystem } from "..";

const fixture = join(import.meta.dir, "fixture");

interface Env {
  GREETING: string;
  COUNTER: any;
  DB: any;
  KV: any;
  FILES: any;
  JOBS: any;
  SELF: any;
}

function files(fs: VirtualFileSystem, dir: string, out: string[] = []): string[] {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const path = `${dir}/${entry.name}`;
    if (entry.isDirectory()) files(fs, path, out);
    else out.push(path);
  }
  return out;
}

async function json(response: Response) {
  expect(response.status).toBe(200);
  return response.json();
}

async function eventually<T>(read: () => Promise<T | null>, timeoutMs = 15_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await read();
    if (value !== null) return value;
    if (Date.now() > deadline) throw new Error("timed out");
    await Bun.sleep(25);
  }
}

async function child(script: string, ...args: string[]) {
  const proc = Bun.spawn([process.execPath, join(import.meta.dir, script), ...args], {
    stdout: "pipe",
    stderr: "inherit",
  });
  const output = await new Response(proc.stdout).text();
  expect(await proc.exited).toBe(0);
  return JSON.parse(output.trim().split("\n").at(-1)!);
}

// A process runs one node, so the suite shares it; the last tests stop it.
let celld: Celld<Env>;
let env: Env;

beforeAll(async () => {
  celld = await Celld.start<Env>({ project: fixture });
  env = celld.env;
});

afterAll(async () => {
  await celld.stop();
});

describe("celld in Bun, storage in node:vfs", () => {
  test("uses Node's vfs implementation", () => {
    expect(vfs.source).toMatch(/node/);
  });

  test("a process runs one node", () => {
    expect(() => new Celld({ project: fixture })).toThrow(/one for its lifetime/);
  });

  test("a Durable Object keeps its key-value and SQL state", async () => {
    expect(await json(await celld.fetch("/?room=lobby"))).toEqual({ n: 1, total: 1 });
    expect(await json(await celld.fetch("/?room=lobby"))).toEqual({ n: 2, total: 2 });
    expect(await json(await celld.fetch("/?room=lobby"))).toEqual({ n: 3, total: 3 });
    expect(await json(await celld.fetch("/?room=kitchen"))).toEqual({ n: 1, total: 1 });
  });

  test("concurrent requests serialize per cell", async () => {
    const rooms = ["a", "b", "c", "d", "e"];
    const responses = await Promise.all(
      Array.from({ length: 40 }, (_, i) => celld.fetch(`/?room=${rooms[i % rooms.length]}`).then(json)),
    );
    for (const room of rooms) {
      const counts = responses.filter((_, i) => rooms[i % rooms.length] === room).map((r) => r.n);
      expect(counts.sort((x, y) => x - y)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    }
  });

  test("D1 writes and reads", async () => {
    await json(await celld.fetch("/guestbook", { method: "POST", body: JSON.stringify({ name: "ada" }) }));
    expect(
      await json(await celld.fetch("/guestbook", { method: "POST", body: JSON.stringify({ name: "grace" }) })),
    ).toEqual(["ada", "grace"]);
  });

  test("an absolute URL keeps its path and query", async () => {
    const response = await celld.dispatchFetch(new Request("https://example.com/?room=lobby"));
    expect(await json(response)).toEqual({ n: 4, total: 4 });
  });
});

describe("the Worker's env, from the host", () => {
  test("vars are the config's strings", () => {
    expect(env.GREETING).toBe("hello");
  });

  test("a Durable Object ID made on the host names the Worker's object", async () => {
    const id = env.COUNTER.idFromName("lobby");
    expect(id.toString()).toBe(await (await celld.fetch("/id?room=lobby")).text());
    expect(id.name).toBe("lobby");
    expect(env.COUNTER.idFromString(id.toString().toUpperCase()).equals(id)).toBe(true);
    expect(() => env.COUNTER.idFromString("0".repeat(64))).toThrow(/not valid for this namespace/);

    const unique = env.COUNTER.newUniqueId();
    expect(await env.COUNTER.get(unique).whoami()).toEqual({ id: unique.toString(), name: null });
  });

  test("RPC and fetch reach one Durable Object", async () => {
    const stub = env.COUNTER.getByName("rpc");
    expect(await stub.increment(5)).toEqual({ n: 5, total: 1 });
    expect(await json(await stub.fetch("http://counter/hit", { method: "POST", body: "x" }))).toEqual({ n: 6, total: 2 });
    expect(await stub.count()).toBe(6);
    expect(await stub.whoami()).toEqual({ id: env.COUNTER.idFromName("rpc").toString(), name: "rpc" });
  });

  test("a Worker's error crosses with its class and message", async () => {
    const error = await env.COUNTER.getByName("rpc").fail().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RangeError);
    expect(error.message).toBe("the counter refuses");
  });

  test("D1", async () => {
    const db = await celld.getD1Database("DB");
    await db.exec("CREATE TABLE IF NOT EXISTS pets (name TEXT PRIMARY KEY, legs INTEGER)");
    const insert = db.prepare("INSERT INTO pets (name, legs) VALUES (?, ?)");
    expect((await insert.bind("cat", 4).run()).meta.changes).toBe(1);
    const [, all] = await db.batch([insert.bind("bird", 2), db.prepare("SELECT * FROM pets ORDER BY name")]);
    expect(all.results).toEqual([{ name: "bird", legs: 2 }, { name: "cat", legs: 4 }]);
    expect(await db.prepare("SELECT legs FROM pets WHERE name = ?").bind("cat").first("legs")).toBe(4);
    expect(await db.prepare("SELECT name, legs FROM pets ORDER BY name").raw()).toEqual([["bird", 2], ["cat", 4]]);
    expect(db.prepare("SELECT * FROM missing").all()).rejects.toThrow(/D1_ERROR: no such table: missing/);
  });

  test("KV", async () => {
    await env.KV.put("user:1", JSON.stringify({ name: "ada" }), { metadata: { role: "admin" } });
    await env.KV.put("user:2", "grace");
    expect(await env.KV.get("user:1", "json")).toEqual({ name: "ada" });
    expect(new TextDecoder().decode(await env.KV.get("user:2", "arrayBuffer"))).toBe("grace");
    const { value, metadata } = await env.KV.getWithMetadata("user:1");
    expect([value, metadata]).toEqual(['{"name":"ada"}', { role: "admin" }]);
    const { keys } = await env.KV.list({ prefix: "user:" });
    expect(keys.map((key: { name: string }) => key.name)).toEqual(["user:1", "user:2"]);
    await env.KV.delete("user:2");
    expect(await env.KV.get("user:2")).toBeNull();
  });

  test("R2", async () => {
    await env.FILES.put("notes/a.txt", "hello r2", {
      httpMetadata: { contentType: "text/plain" },
      customMetadata: { author: "ada" },
    });
    const object = await env.FILES.get("notes/a.txt");
    expect(await object.text()).toBe("hello r2");
    expect([object.size, object.customMetadata]).toEqual([8, { author: "ada" }]);
    const headers = new Headers();
    object.writeHttpMetadata(headers);
    expect(headers.get("content-type")).toBe("text/plain");
    expect(object.checksums.toJSON().md5).toMatch(/^[0-9a-f]{32}$/);
    const { objects } = await env.FILES.list({ prefix: "notes/" });
    expect(objects.map((o: { key: string }) => o.key)).toEqual(["notes/a.txt"]);
    expect((await env.FILES.head("notes/a.txt")).etag).toBe(object.etag);
    await env.FILES.delete("notes/a.txt");
    expect(await env.FILES.head("notes/a.txt")).toBeNull();
  });

  test("a queued message reaches the Worker's consumer", async () => {
    await env.JOBS.send({ key: "welcome", to: "ada" });
    const job = await eventually(() => env.KV.get("job:welcome", "json"));
    expect(job).toEqual({ key: "welcome", to: "ada", attempts: 1 });
  });

  test("a service binding's RPC reaches its entrypoint", async () => {
    expect(await env.SELF.greet("ada")).toBe("hello, ada");
  });
});

describe("redeploy", () => {
  test("changes the Worker and keeps every cell's state", async () => {
    await celld.redeploy({ bindings: { GREETING: "howdy" } });
    expect(await (await celld.fetch("/greeting")).text()).toBe("howdy");
    expect(celld.env.GREETING).toBe("howdy");
    expect(await celld.env.SELF.greet("ada")).toBe("howdy, ada");
    expect(await celld.env.COUNTER.getByName("rpc").count()).toBe(6);
    expect(await json(await celld.fetch("/?room=lobby"))).toEqual({ n: 5, total: 5 });
  });

  test("refuses to change the node's storage", async () => {
    expect(celld.setOptions({ project: fixture, persist: tmpdir() })).rejects.toThrow(/persist is fixed/);
  });
});

describe("storage", () => {
  test("every file the node writes is in the VFS", () => {
    const bucket = files(celld.vfs, celld.bucket);
    const state = files(celld.vfs, celld.state);
    expect(bucket).toContain(`${celld.bucket}/deploy/current.json`);
    expect(bucket.some((path) => /\/cells\/Counter:[^/]+\/own\.json$/.test(path))).toBe(true);
    expect(bucket.some((path) => /\/cells\/__D1Database:[^/]+\/ltx\/.+\.ltx$/.test(path))).toBe(true);
    expect(bucket.some((path) => /\/cells\/__KvNamespace:[^/]+\/ltx\/.+\.ltx$/.test(path))).toBe(true);
    expect(state.some((path) => /\/Counter:[^/]+\/ltx\/e\d+\/db\.sqlite$/.test(path))).toBe(true);
    expect(existsSync(celld.bucket)).toBe(false);
    expect(existsSync(celld.state)).toBe(false);
  });

  test("a running node cannot be snapshotted", async () => {
    expect(celld.snapshot()).rejects.toThrow(/stop the node/);
  });

  test("a snapshot restores every cell and the deployment in a fresh process", async () => {
    const started = performance.now();
    const exit = await celld.stop();
    expect(exit.code).toBe(0);
    // An embedded node has no successor to wait for.
    expect(performance.now() - started).toBeLessThan(1_000);

    const snapshot = await celld.snapshot();
    const file = join(mkdtempSync(join(tmpdir(), "celld-napi-")), "snapshot.json");
    writeFileSync(file, JSON.stringify(snapshot));
    const restored = await child("restore.ts", file);
    expect(restored.lobby).toEqual({ n: 6, total: 6 });
    expect(restored.guestbook).toEqual(["ada", "grace"]);
    expect(restored.rpc).toBe(6);
    expect(restored.user).toEqual({ name: "ada" });
    expect(restored.greeting).toBe("howdy, ada");
    expect(restored.stateRebuilt).toBe(true);
    expect(restored.exit.code).toBe(0);
    // The snapshot leaves out the stopped node's fleet records, so the
    // restored node has no peer to wait on.
    expect(restored.stopMs).toBeLessThan(1_000);
    expect(Object.keys(snapshot.files).some((name) => name.startsWith("nodes/"))).toBe(false);
  });
});

describe("Miniflare-style options", () => {
  test("an inline Worker, setOptions, and await using", async () => {
    const result = await child("inline.ts");
    expect(result.first).toBe("hi from /path?q=1");
    expect(result.cached).toBe("cached");
    expect(result.members).toEqual(["ada"]);
    expect(result.second).toBe("hey via /path?q=1");
    expect(result.membersAfter).toEqual(["ada", "grace"]);
    expect(result.cachedAfter).toBe("cached");
    expect(result.bindings).toEqual(["CACHE", "GREETING", "ROOMS"]);
    expect(result.exited.code).toBe(0);
  });

  test("persist keeps storage on disk for the next process", async () => {
    const dir = mkdtempSync(join(tmpdir(), "celld-persist-"));
    const first = await child("persist.ts", dir, "write");
    expect(first).toMatchObject({ notes: ["from write"], theme: "dark", onDisk: true, exit: { code: 0 } });
    const second = await child("persist.ts", dir, "read");
    expect(second).toMatchObject({ notes: ["from write", "from read"], theme: "dark", exit: { code: 0 } });
  });
});
