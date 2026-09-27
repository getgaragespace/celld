// A Worker given inline, in Miniflare's option names, then replaced with
// setOptions. Run by celld.test.ts, because a process runs one node.
import { Celld } from "..";

const script = (verb: string) => `
import { DurableObject } from "cloudflare:workers";

export class Room extends DurableObject {
  async join(who) {
    const members = (await this.ctx.storage.get("members")) ?? [];
    members.push(who);
    await this.ctx.storage.put("members", members);
    return members;
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/cache") return new Response(await env.CACHE.get("k"));
    return new Response(\`\${env.GREETING} ${verb} \${url.pathname}\${url.search}\`);
  },
};
`;

const options = {
  modules: true,
  script: script("from"),
  compatibilityDate: "2026-01-01",
  durableObjects: { ROOMS: "Room" },
  kvNamespaces: ["CACHE"],
  bindings: { GREETING: "hi" },
};

let result: Record<string, unknown>;
let exited: Promise<unknown>;
{
  await using celld = await Celld.start<any>(options);
  exited = celld.exited;
  const first = await (await celld.dispatchFetch("http://example.com/path?q=1")).text();
  await (await celld.getKVNamespace("CACHE")).put("k", "cached");
  const cached = await (await celld.fetch("/cache")).text();
  const members = await celld.env.ROOMS.getByName("a").join("ada");

  await celld.setOptions({ ...options, script: script("via"), bindings: { GREETING: "hey" } });
  const second = await (await celld.fetch("/path?q=1")).text();
  const membersAfter = await celld.env.ROOMS.getByName("a").join("grace");
  const cachedAfter = await celld.env.CACHE.get("k");
  const bindings = Object.keys(await celld.getBindings()).sort();
  result = { first, cached, members, second, membersAfter, cachedAfter, bindings };
}
// Leaving the block disposed of the node.
console.log(JSON.stringify({ ...result, exited: await exited }));
