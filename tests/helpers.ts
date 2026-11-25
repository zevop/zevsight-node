import { gunzipSync } from 'node:zlib';

export interface Recorded {
  url: string;
  headers: Record<string, string>;
  events: any[];
}

type Reply = number | { status: number; body?: unknown; headers?: Record<string, string> } | Error;

/**
 * A fetch that records each request and answers from a script. When the
 * script runs out, every further request gets the last reply.
 */
export function scriptedFetch(script: Reply[] = [202]) {
  const calls: Recorded[] = [];
  let i = 0;
  const fn = (async (url: string, init: RequestInit) => {
    const headers = Object.fromEntries(
      Object.entries((init.headers ?? {}) as Record<string, string>).map(([k, v]) => [k.toLowerCase(), v]),
    );
    const raw = init.body as string | Uint8Array;
    const text =
      headers['content-encoding'] === 'gzip' ? gunzipSync(raw as Uint8Array).toString('utf8') : String(raw);
    calls.push({ url: String(url), headers, events: JSON.parse(text).events });
    const reply = script[Math.min(i++, script.length - 1)]!;
    if (reply instanceof Error) throw reply;
    const r = typeof reply === 'number' ? { status: reply } : reply;
    const body =
      r.body !== undefined
        ? JSON.stringify(r.body)
        : r.status === 202
          ? JSON.stringify({ accepted: JSON.parse(text).events.length })
          : JSON.stringify({ code: 'X', message: `status ${r.status}` });
    return new Response(body, { status: r.status, headers: r.headers });
  }) as unknown as typeof fetch;
  return { fetch: fn, calls };
}
