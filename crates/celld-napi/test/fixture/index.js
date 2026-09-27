import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

// Both storage APIs of a SQLite-backed Durable Object: the key-value API and
// the SQL API. `fetch` and the RPC methods share the one counter.
export class Counter extends DurableObject {
  async increment(by = 1, path = "rpc") {
    const n = ((await this.ctx.storage.get("n")) ?? 0) + by;
    await this.ctx.storage.put("n", n);
    const sql = this.ctx.storage.sql;
    sql.exec("CREATE TABLE IF NOT EXISTS hits (n INTEGER PRIMARY KEY, path TEXT)");
    sql.exec("INSERT INTO hits (n, path) VALUES (?, ?)", n, path);
    const [{ total }] = sql.exec("SELECT count(*) AS total FROM hits").toArray();
    return { n, total };
  }

  async count() {
    return (await this.ctx.storage.get("n")) ?? 0;
  }

  whoami() {
    return { id: this.ctx.id.toString(), name: this.ctx.id.name ?? null };
  }

  fail() {
    throw new RangeError("the counter refuses");
  }

  async fetch(request) {
    return Response.json(await this.increment(1, new URL(request.url).pathname));
  }
}

export class Greeter extends WorkerEntrypoint {
  greet(name) {
    return `${this.env.GREETING}, ${name}`;
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
    const room = url.searchParams.get("room") ?? "lobby";
    switch (url.pathname) {
      case "/guestbook": {
        await migrate(env);
        if (request.method === "POST") {
          const { name } = await request.json();
          await env.DB.prepare("INSERT INTO entries (name) VALUES (?)").bind(name).run();
        }
        const { results } = await env.DB.prepare("SELECT name FROM entries ORDER BY id").all();
        return Response.json(results.map((row) => row.name));
      }
      case "/greeting":
        return new Response(env.GREETING);
      case "/id":
        return new Response(env.COUNTER.idFromName(room).toString());
      default:
        return env.COUNTER.get(env.COUNTER.idFromName(room)).fetch(request);
    }
  },

  async queue(batch, env) {
    for (const message of batch.messages) {
      await env.KV.put(`job:${message.body.key}`, JSON.stringify({ ...message.body, attempts: message.attempts }));
      message.ack();
    }
  },
};
