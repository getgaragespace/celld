// Start a node in this process from a stopped node's snapshot alone, and
// report what the cells contain. Run by celld.test.ts.
import { readFileSync } from "node:fs";
import { Celld } from "..";

const snapshot = JSON.parse(readFileSync(process.argv[2], "utf8"));
const celld = await Celld.start<any>({ snapshot });
const lobby = await (await celld.fetch("/?room=lobby")).json();
const guestbook = await (await celld.fetch("/guestbook")).json();
const rpc = await celld.env.COUNTER.getByName("rpc").count();
const user = await celld.env.KV.get("user:1", "json");
const greeting = await celld.env.SELF.greet("ada");
const stateRebuilt = celld.vfs.readdirSync(celld.state).some((name) => name.startsWith("Counter:"));
const stopping = performance.now();
const exit = await celld.stop();
const stopMs = performance.now() - stopping;
console.log(JSON.stringify({ lobby, guestbook, rpc, user, greeting, stateRebuilt, exit, stopMs }));
