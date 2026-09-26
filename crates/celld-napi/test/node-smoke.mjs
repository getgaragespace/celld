// The same node under Node, on Node's own node:vfs. Run with
// `node --experimental-vfs test/node-smoke.mjs` (Node v26 or later).
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const require = createRequire(import.meta.url);
const { startCelld, vfs } = require("..");

test("celld in Node, storage in node:vfs", async () => {
  assert.equal(vfs.source, "node");
  const celld = await startCelld({ project: fileURLToPath(new URL("./fixture", import.meta.url)) });
  for (const n of [1, 2, 3]) {
    assert.deepEqual(await (await celld.fetch("/?room=lobby")).json(), { n, total: n });
  }
  const post = { method: "POST", body: JSON.stringify({ name: "ada" }) };
  assert.deepEqual(await (await celld.fetch("/guestbook", post)).json(), ["ada"]);
  assert.ok(celld.vfs.existsSync(`${celld.bucket}/deploy/current.json`));
  assert.equal((await celld.stop()).code, 0);
});
