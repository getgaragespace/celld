// The same node under Node, on Node's own node:vfs. Run with
// `node --experimental-vfs test/node-smoke.mjs` (Node v26 or later).
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const require = createRequire(import.meta.url);
const { Celld, vfs } = require("..");

test("celld in Node, storage in node:vfs", async () => {
  assert.equal(vfs.source, "node");
  await using celld = await Celld.start({ project: fileURLToPath(new URL("./fixture", import.meta.url)) });
  for (const n of [1, 2, 3]) {
    assert.deepEqual(await (await celld.fetch("/?room=lobby")).json(), { n, total: n });
  }
  const post = { method: "POST", body: JSON.stringify({ name: "ada" }) };
  assert.deepEqual(await (await celld.fetch("/guestbook", post)).json(), ["ada"]);
  assert.ok(celld.vfs.existsSync(`${celld.bucket}/deploy/current.json`));

  const { COUNTER, KV, SELF } = celld.env;
  assert.deepEqual(await COUNTER.getByName("lobby").increment(10), { n: 13, total: 4 });
  await KV.put("k", "v");
  assert.equal(await KV.get("k"), "v");
  assert.equal(await SELF.greet("node"), "hello, node");
  assert.equal((await celld.stop()).code, 0);
});
