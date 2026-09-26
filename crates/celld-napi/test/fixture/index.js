import { DurableObject } from "cloudflare:workers";

// Both storage APIs of a SQLite-backed Durable Object: the key-value API and
// the SQL API.
export class Counter extends DurableObject {
  async fetch(request) {
    const n = ((await this.ctx.storage.get("n")) ?? 0) + 1;
    await this.ctx.storage.put("n", n);
    const sql = this.ctx.storage.sql;
    sql.exec("CREATE TABLE IF NOT EXISTS hits (n INTEGER PRIMARY KEY, path TEXT)");
    sql.exec("INSERT INTO hits (n, path) VALUES (?, ?)", n, new URL(request.url).pathname);
    const [{ total }] = sql.exec("SELECT count(*) AS total FROM hits").toArray();
    return Response.json({ n, total });
  }
}

let ready;
const migrate = (env) =>
  (ready ??= env.DB.exec(
    "CREATE TABLE IF NOT EXISTS entries (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
  ).catch((error) => {
    ready = undefined;
    throw error;
  }));

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/guestbook") {
      await migrate(env);
      if (request.method === "POST") {
        const { name } = await request.json();
        await env.DB.prepare("INSERT INTO entries (name) VALUES (?)").bind(name).run();
      }
      const { results } = await env.DB.prepare("SELECT name FROM entries ORDER BY id").all();
      return Response.json(results.map((row) => row.name));
    }
    const room = url.searchParams.get("room") ?? "lobby";
    return env.COUNTER.get(env.COUNTER.idFromName(room)).fetch(request);
  },
};
