// A node whose storage is a directory on disk. `write` leaves state there;
// `read`, in a later process, finds it. Run by celld.test.ts.
import { existsSync } from "node:fs";
import { join } from "node:path";
import { Celld } from "..";

const [dir, mode] = process.argv.slice(2);
const script = `
import { DurableObject } from "cloudflare:workers";
export class Notes extends DurableObject {
  add(note) { const notes = this.ctx.storage.kv.get("notes") ?? []; notes.push(note); this.ctx.storage.kv.put("notes", notes); return notes; }
}
export default { fetch: () => new Response("ok") };
`;
const celld = await Celld.start<any>({
  script,
  compatibilityDate: "2026-01-01",
  durableObjects: { NOTES: "Notes" },
  kvNamespaces: { SETTINGS: "settings" },
  persist: dir,
});
const notes = await celld.env.NOTES.getByName("mine").add(`from ${mode}`);
if (mode === "write") await celld.env.SETTINGS.put("theme", "dark");
const theme = await celld.env.SETTINGS.get("theme");
const exit = await celld.stop();
const onDisk = existsSync(join(dir, "bucket", "deploy", "current.json"));
console.log(JSON.stringify({ notes, theme, onDisk, exit }));
