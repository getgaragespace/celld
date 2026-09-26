import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { startCelld, vfs, type Celld, type VirtualFileSystem } from "..";

const fixture = join(import.meta.dir, "fixture");

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

// A process runs one node, so the suite shares it; the last test stops it.
let celld: Celld;

beforeAll(async () => {
  celld = await startCelld({ project: fixture });
});

afterAll(async () => {
  await celld.stop();
});

describe("celld in Bun, storage in node:vfs", () => {
  test("uses Node's vfs implementation", () => {
    expect(vfs.source).toMatch(/node/);
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

  test("every file the node writes is in the VFS", () => {
    const bucket = files(celld.vfs, celld.bucket);
    const state = files(celld.vfs, celld.state);
    expect(bucket).toContain(`${celld.bucket}/deploy/current.json`);
    expect(bucket.some((path) => /\/cells\/Counter:[^/]+\/own\.json$/.test(path))).toBe(true);
    expect(bucket.some((path) => /\/cells\/__D1Database:[^/]+\/ltx\/.+\.ltx$/.test(path))).toBe(true);
    expect(state.some((path) => /\/Counter:[^/]+\/ltx\/e\d+\/db\.sqlite$/.test(path))).toBe(true);
    expect(existsSync(celld.bucket)).toBe(false);
    expect(existsSync(celld.state)).toBe(false);
  });

  test("the bucket alone restores every cell in a fresh process", async () => {
    const exit = await celld.stop();
    expect(exit.code).toBe(0);

    const snapshot: Record<string, string> = {};
    for (const path of files(celld.vfs, celld.bucket)) {
      snapshot[path] = celld.vfs.readFileSync(path).toString("base64");
    }
    const file = join(mkdtempSync(join(tmpdir(), "celld-napi-")), "bucket.json");
    writeFileSync(file, JSON.stringify(snapshot));

    const child = Bun.spawn([process.execPath, join(import.meta.dir, "restore.ts"), file], {
      stdout: "pipe",
      stderr: "inherit",
    });
    const output = await new Response(child.stdout).text();
    expect(await child.exited).toBe(0);
    const restored = JSON.parse(output.trim().split("\n").at(-1)!);
    expect(restored.lobby).toEqual({ n: 4, total: 4 });
    expect(restored.guestbook).toEqual(["ada", "grace"]);
    expect(restored.stateRebuilt).toBe(true);
    expect(restored.exit.code).toBe(0);
  });
});
