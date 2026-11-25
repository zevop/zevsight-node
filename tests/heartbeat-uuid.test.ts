import { describe, expect, it } from 'vitest';
import { heartbeat, uuidv7, withHeartbeat } from '../src/index';

function recordingFetch(status = 200) {
  const calls: Array<{ url: string; method: string; body?: string }> = [];
  const fn = (async (url: string, init: RequestInit) => {
    calls.push({ url: String(url), method: String(init.method), body: init.body as string | undefined });
    return new Response('OK', { status });
  }) as unknown as typeof fetch;
  return { fn, calls };
}

describe('heartbeat', () => {
  const URL = 'https://ingest.zevsight.com/h/hb_abc';

  it('pings start, ok and fail with notes', async () => {
    const f = recordingFetch();
    const hb = heartbeat(URL + '/', { fetch: f.fn });
    expect(await hb.start()).toBe(true);
    expect(await hb.ok()).toBe(true);
    expect(await hb.fail('disk full')).toBe(true);
    expect(f.calls).toEqual([
      { url: URL + '/start', method: 'GET', body: undefined },
      { url: URL, method: 'GET', body: undefined },
      { url: URL + '/fail', method: 'POST', body: 'disk full' },
    ]);
  });

  it('never throws and does not retry an unknown token', async () => {
    const f = recordingFetch(404);
    expect(await heartbeat(URL, { fetch: f.fn }).ok()).toBe(false);
    expect(f.calls).toHaveLength(1);
    const broken = (async () => {
      throw new Error('offline');
    }) as unknown as typeof fetch;
    expect(await heartbeat(URL, { fetch: broken, retries: 0 }).ok()).toBe(false);
  });

  it('wraps a job: start then ok, or start then fail and rethrow', async () => {
    const f = recordingFetch();
    expect(await withHeartbeat(URL, async () => 42, { fetch: f.fn })).toBe(42);
    await expect(
      withHeartbeat(URL, async () => {
        throw new Error('export failed');
      }, { fetch: f.fn }),
    ).rejects.toThrow('export failed');
    expect(f.calls.map((c) => c.url.replace(URL, '') || 'ok')).toEqual(['/start', 'ok', '/start', '/fail']);
    expect(f.calls[3]!.body).toBe('Error: export failed');
  });
});

describe('uuidv7', () => {
  it('is a version 7 UUID and sorts in creation order', () => {
    const ids = Array.from({ length: 5000 }, () => uuidv7());
    for (const id of ids.slice(0, 50)) expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    expect([...ids].sort()).toEqual(ids);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('encodes the timestamp', () => {
    // Ahead of every id made so far, so the monotonic guard does not apply.
    const ms = Date.now() + 365 * 24 * 3600 * 1000;
    const id = uuidv7(ms);
    expect(parseInt(id.replace(/-/g, '').slice(0, 12), 16)).toBe(ms);
  });
});
