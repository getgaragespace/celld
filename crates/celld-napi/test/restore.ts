// Start a node in this process on a fresh VFS that holds only a bucket
// snapshot, and report what the cells contain. Run by celld.test.ts.
import { readFileSync } from "node:fs";
import { dirname } from "node:path";
import { startCelld, vfs } from "..";

const snapshot: Record<string, string> = JSON.parse(readFileSync(process.argv[2], "utf8"));
const fs = vfs.create(new vfs.MemoryProvider(), { emitExperimentalWarning: false });
for (const [path, base64] of Object.entries(snapshot)) {
  fs.mkdirSync(dirname(path), { recursive: true });
  fs.writeFileSync(path, Buffer.from(base64, "base64"));
}

const celld = await startCelld({ vfs: fs });
const lobby = await (await celld.fetch("/?room=lobby")).json();
const guestbook = await (await celld.fetch("/guestbook")).json();
const stateRebuilt = fs.readdirSync(celld.state).some((name) => name.startsWith("Counter:"));
const exit = await celld.stop();
console.log(JSON.stringify({ lobby, guestbook, stateRebuilt, exit }));
