/**
 * Heartbeats for cron jobs and workers.
 *
 * A heartbeat monitor in ZevSight gives you a ping URL. Ping it when a job
 * runs; ZevSight alerts when a ping is late or reports a failure. The URL
 * itself is the credential, so no API key is needed.
 *
 *   const hb = heartbeat(process.env.ZEVSIGHT_HEARTBEAT_NIGHTLY_EXPORT!);
 *   await hb.start();
 *   …
 *   await hb.ok();
 *
 * Pinging never throws: a monitoring outage must not fail your job. Each
 * method resolves to true when ZevSight recorded the ping.
 */
export interface HeartbeatOptions {
  /** Per-ping timeout. Default 10 s. */
  timeoutMs?: number;
  /** Retries on network errors and 5xx. Default 2. */
  retries?: number;
  /** For tests. Defaults to the global fetch. */
  fetch?: typeof fetch;
}

export interface Heartbeat {
  /** The job started. ZevSight measures the run from here to ok/fail. */
  start(): Promise<boolean>;
  /** The job finished well. An optional note (≤ 8 KiB) is stored with the ping. */
  ok(note?: string): Promise<boolean>;
  /** The job failed. ZevSight alerts at once, without waiting for the grace period. */
  fail(note?: string): Promise<boolean>;
}

const NOTE_MAX = 8192;

export function heartbeat(url: string, options: HeartbeatOptions = {}): Heartbeat {
  const base = url.trim().replace(/\/+$/, '');
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const timeoutMs = options.timeoutMs ?? 10_000;
  const retries = options.retries ?? 2;

  async function ping(suffix: '' | '/start' | '/fail', note?: string): Promise<boolean> {
    if (!base) return false;
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        const res = await fetchImpl(base + suffix, {
          method: note ? 'POST' : 'GET',
          headers: note ? { 'content-type': 'text/plain; charset=utf-8' } : undefined,
          body: note ? truncate(note) : undefined,
          signal: AbortSignal.timeout(timeoutMs),
        });
        if (res.ok) return true;
        if (res.status < 500) return false; // unknown token, wrong region: retrying will not help
      } catch {
        // network error: retry below
      }
      if (attempt < retries) await new Promise((r) => setTimeout(r, 500 * 2 ** attempt));
    }
    return false;
  }

  return {
    start: () => ping('/start'),
    ok: (note) => ping('', note),
    fail: (note) => ping('/fail', note ?? 'failed'),
  };
}

/**
 * Runs `fn` between a start and an ok ping, or a fail ping (with the
 * error's message) if it throws. Returns what `fn` returns and rethrows
 * what it throws.
 */
export async function withHeartbeat<T>(
  url: string,
  fn: () => Promise<T> | T,
  options?: HeartbeatOptions,
): Promise<T> {
  const hb = heartbeat(url, options);
  await hb.start();
  try {
    const result = await fn();
    await hb.ok();
    return result;
  } catch (err) {
    await hb.fail(err instanceof Error ? `${err.name}: ${err.message}` : String(err));
    throw err;
  }
}

function truncate(note: string): string {
  if (Buffer.byteLength(note) <= NOTE_MAX) return note;
  let s = note.slice(0, NOTE_MAX);
  while (Buffer.byteLength(s) > NOTE_MAX) s = s.slice(0, -1);
  return s;
}
