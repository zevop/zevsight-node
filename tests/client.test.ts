import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { DROPPED_ATTR, ZevSight, type ZevSightError } from '../src/index';
import { backoff, retryAfterMs, slug } from '../src/client';
import { scriptedFetch } from './helpers';

const V7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

function client(fetchImpl: typeof fetch, extra: Partial<ConstructorParameters<typeof ZevSight>[0]> = {}) {
  const errors: ZevSightError[] = [];
  const c = new ZevSight({
    apiKey: 'zsi_test',
    endpoint: 'http://zs.test',
    service: 'api',
    environment: 'test',
    handleExit: false,
    fetch: fetchImpl,
    onError: (e) => errors.push(e),
    ...extra,
  });
  return { c, errors };
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe('batching', () => {
  it('splits by event count and keeps order', async () => {
    const f = scriptedFetch();
    const { c } = client(f.fetch, { maxBatchEvents: 3 });
    for (let i = 0; i < 7; i++) c.info(`line ${i}`);
    await c.flush();
    expect(f.calls.map((x) => x.events.length)).toEqual([3, 3, 1]);
    expect(f.calls.flatMap((x) => x.events.map((e) => e.body.message))).toEqual(
      Array.from({ length: 7 }, (_, i) => `line ${i}`),
    );
    await c.close();
  });

  it('sends on the interval when the batch is not full', async () => {
    const f = scriptedFetch();
    const { c } = client(f.fetch, { flushIntervalMs: 1000 });
    c.info('hello');
    await vi.advanceTimersByTimeAsync(999);
    expect(f.calls).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.calls).toHaveLength(1);
    await c.close();
  });

  it('sends as soon as the byte limit is reached', async () => {
    const f = scriptedFetch();
    const { c } = client(f.fetch, { maxBatchBytes: 2000 });
    for (let i = 0; i < 20; i++) c.info('x'.repeat(200));
    // The byte limit took a batch at once, without waiting for the interval.
    expect(c.pending).toBeLessThan(20);
    await c.flush();
    expect(f.calls.flatMap((x) => x.events)).toHaveLength(20);
    for (const call of f.calls) expect(JSON.stringify(call.events).length).toBeLessThan(2600);
    await c.close();
  });

  it('gzips larger bodies and sets the headers', async () => {
    const f = scriptedFetch();
    const { c } = client(f.fetch);
    for (let i = 0; i < 20; i++) c.info('a fairly long log line number ' + i);
    await c.flush();
    const h = f.calls[0]!.headers;
    expect(h['content-encoding']).toBe('gzip');
    expect(h.authorization).toBe('Bearer zsi_test');
    expect(h['idempotency-key']).toMatch(/^zsn-/);
    expect(f.calls[0]!.url).toBe('http://zs.test/v1/ingest/events');
    await c.close();
  });

  it('builds envelope events', async () => {
    const f = scriptedFetch();
    const { c } = client(f.fetch, { resource: { 'host.name': 'web-1', version: 3 } });
    c.log({
      message: 'paid',
      severity: 'warn',
      attrs: { amount: 1200, ok: true, obj: { a: 1 }, nil: null },
      fields: { cart: { items: 2 } },
      traceId: '4BF92F3577B34DA6A3CE929D0E0E4736',
      spanId: 'nope',
      ts: new Date('2026-11-25T10:00:00Z'),
    });
    await c.flush();
    const ev = f.calls[0]!.events[0];
    expect(ev.id).toMatch(V7);
    expect(ev).toMatchObject({
      type: 'log',
      service: 'api',
      environment: 'test',
      severity: 'warn',
      ts: '2026-11-25T10:00:00.000Z',
      trace_id: '4bf92f3577b34da6a3ce929d0e0e4736',
      attrs: { amount: '1200', ok: 'true', obj: '{"a":1}', span_id: 'nope' },
      resource: { 'host.name': 'web-1', version: '3' },
      body: { message: 'paid', fields: { cart: { items: 2 } } },
    });
    expect(ev.attrs.nil).toBeUndefined();
    expect(ev.span_id).toBeUndefined();
    await c.close();
  });

  it('keeps an unknown level as an attribute instead of losing the line', async () => {
    const f = scriptedFetch();
    const { c } = client(f.fetch);
    c.log({ message: 'x', severity: 'loud' as never });
    await c.flush();
    expect(f.calls[0]!.events[0].severity).toBeUndefined();
    expect(f.calls[0]!.events[0].attrs.level).toBe('loud');
    await c.close();
  });
});

describe('retries', () => {
  it('retries 5xx with backoff, sending the identical batch', async () => {
    const f = scriptedFetch([503, 500, 202]);
    const { c, errors } = client(f.fetch);
    c.info('one');
    c.info('two');
    const done = c.flush();
    await vi.advanceTimersByTimeAsync(60_000);
    await done;
    expect(f.calls).toHaveLength(3);
    const keys = new Set(f.calls.map((x) => x.headers['idempotency-key']));
    expect(keys.size).toBe(1);
    const ids = f.calls.map((x) => x.events.map((e) => e.id).join());
    expect(new Set(ids).size).toBe(1);
    expect(errors).toEqual([]);
    await c.close();
  });

  it('retries network errors and 408', async () => {
    const f = scriptedFetch([new TypeError('fetch failed'), 408, 202]);
    const { c, errors } = client(f.fetch);
    c.info('x');
    const done = c.flush();
    await vi.advanceTimersByTimeAsync(60_000);
    await done;
    expect(f.calls).toHaveLength(3);
    expect(errors).toEqual([]);
    await c.close();
  });

  it('waits exactly as long as Retry-After says', async () => {
    const f = scriptedFetch([{ status: 429, headers: { 'retry-after': '7' } }, 202]);
    const { c } = client(f.fetch);
    c.info('x');
    const done = c.flush();
    await vi.advanceTimersByTimeAsync(6_999);
    expect(f.calls).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(2);
    expect(f.calls).toHaveLength(2);
    await done;
    await c.close();
  });

  it('gives up when Retry-After is longer than maxRetryAfterMs (quota exceeded)', async () => {
    const f = scriptedFetch([
      { status: 429, headers: { 'retry-after': '86400' }, body: { code: 'QUOTA_EXCEEDED', message: 'over quota' } },
    ]);
    const { c, errors } = client(f.fetch);
    c.info('x');
    await c.flush();
    expect(f.calls).toHaveLength(1);
    expect(errors[0]).toMatchObject({ kind: 'delivery_failed', status: 429, code: 'QUOTA_EXCEEDED', events: 1 });
    await c.close();
  });

  it('gives up after maxRetries', async () => {
    const f = scriptedFetch([503]);
    const { c, errors } = client(f.fetch, { maxRetries: 3 });
    c.info('x');
    const done = c.flush();
    await vi.advanceTimersByTimeAsync(120_000);
    await done;
    expect(f.calls).toHaveLength(4);
    expect(errors[0]).toMatchObject({ kind: 'delivery_failed', status: 503 });
    await c.close();
  });

  it('does not retry other 4xx, and reports the right region on 421', async () => {
    for (const [status, body] of [
      [401, { code: 'UNAUTHENTICATED', message: 'bad key' }],
      [421, { code: 'WRONG_REGION', message: 'other region', endpoint: 'https://us.ingest.zevsight.com' }],
    ] as const) {
      const f = scriptedFetch([{ status, body }]);
      const { c, errors } = client(f.fetch);
      c.info('x');
      await c.flush();
      expect(f.calls).toHaveLength(1);
      expect(errors[0]).toMatchObject({ kind: 'delivery_failed', status, code: body.code });
      if (status === 421) expect(errors[0]).toMatchObject({ endpoint: 'https://us.ingest.zevsight.com' });
      await c.close();
    }
  });

  it('splits a batch the server says is too large', async () => {
    const f = scriptedFetch([413, 202, 202]);
    const { c } = client(f.fetch);
    for (let i = 0; i < 4; i++) c.info(`l${i}`);
    await c.flush();
    expect(f.calls.map((x) => x.events.length)).toEqual([4, 2, 2]);
    const k = f.calls[0]!.headers['idempotency-key'];
    expect(f.calls[1]!.headers['idempotency-key']).toBe(`${k}.1`);
    expect(f.calls[2]!.headers['idempotency-key']).toBe(`${k}.2`);
    await c.close();
  });
});

describe('refused events', () => {
  it('reports per-event refusals with the event', async () => {
    const f = scriptedFetch([
      { status: 202, body: { accepted: 1, rejected: [{ index: 1, field: 'ts', message: 'Older than retention' }] } },
    ]);
    const { c, errors } = client(f.fetch);
    c.info('kept');
    c.info('refused');
    await c.flush();
    expect(errors).toHaveLength(1);
    const e = errors[0]!;
    expect(e.kind).toBe('rejected');
    if (e.kind === 'rejected') {
      expect(e.rejected[0]!.field).toBe('ts');
      expect(e.rejected[0]!.event.body.message).toBe('refused');
    }
    await c.close();
  });

  it('does not retry a 400 where every event was refused', async () => {
    const f = scriptedFetch([{ status: 400, body: { code: 'INVALID_REQUEST', message: 'x', rejected: [{ index: 0, message: 'bad' }] } }]);
    const { c, errors } = client(f.fetch);
    c.info('x');
    await c.flush();
    expect(f.calls).toHaveLength(1);
    expect(errors[0]!.kind).toBe('rejected');
    await c.close();
  });

  it('surfaces quota warnings', async () => {
    const f = scriptedFetch([{ status: 202, body: { accepted: 1, quota: { used_ratio: 0.85, state: 'warning' } } }]);
    const quotas: unknown[] = [];
    const { c } = client(f.fetch, { onQuota: (q) => quotas.push(q) });
    c.info('x');
    await c.flush();
    expect(quotas).toEqual([{ usedRatio: 0.85, state: 'warning' }]);
    expect(c.quota?.state).toBe('warning');
    await c.close();
  });
});

describe('buffer', () => {
  it('drops the oldest beyond maxBufferEvents and says so on the next batch', async () => {
    const f = scriptedFetch();
    const { c, errors } = client(f.fetch, { maxBufferEvents: 5, maxBatchEvents: 100 });
    for (let i = 0; i < 8; i++) c.info(`l${i}`);
    expect(c.pending).toBe(5);
    await c.flush();
    const evs = f.calls[0]!.events;
    expect(evs.map((e) => e.body.message)).toEqual(['l3', 'l4', 'l5', 'l6', 'l7']);
    expect(evs[0].attrs[DROPPED_ATTR]).toBe('3');
    expect(errors).toContainEqual({ kind: 'dropped', events: 3 });
    await c.close();
  });
});

describe('shutdown', () => {
  it('close() sends what is pending', async () => {
    const f = scriptedFetch();
    const { c } = client(f.fetch, { flushIntervalMs: 60_000 });
    c.info('last words');
    await c.close();
    expect(f.calls.flatMap((x) => x.events.map((e) => e.body.message))).toEqual(['last words']);
  });

  it('close() gives up at its deadline and reports what was lost', async () => {
    const f = scriptedFetch([503]);
    const { c, errors } = client(f.fetch);
    c.info('a');
    c.info('b');
    const closing = c.close(2_000);
    await vi.advanceTimersByTimeAsync(3_000);
    await closing;
    expect(errors.some((e) => e.kind === 'delivery_failed' && e.events === 2)).toBe(true);
  });

  it('ignores lines logged after close', async () => {
    const f = scriptedFetch();
    const { c } = client(f.fetch);
    await c.close();
    c.info('too late');
    expect(c.pending).toBe(0);
  });
});

describe('configuration', () => {
  it('reads defaults from the environment', () => {
    vi.stubEnv('ZEVSIGHT_INGEST_KEY', 'zsi_env');
    vi.stubEnv('OTEL_SERVICE_NAME', 'Billing Worker');
    vi.stubEnv('NODE_ENV', 'staging');
    vi.stubEnv('ZEVSIGHT_REGION', '');
    vi.stubEnv('ZEVSIGHT_ENDPOINT', '');
    const c = new ZevSight({ handleExit: false, fetch: scriptedFetch().fetch });
    expect(c.service).toBe('billing-worker');
    expect(c.environment).toBe('staging');
    expect(c.endpoint).toBe('https://ingest.zevsight.com');
    expect(new ZevSight({ apiKey: 'k', region: 'us', handleExit: false }).endpoint).toBe(
      'https://us.ingest.zevsight.com',
    );
    vi.unstubAllEnvs();
  });

  it('does nothing without a key', async () => {
    vi.stubEnv('ZEVSIGHT_INGEST_KEY', '');
    const f = scriptedFetch();
    const c = new ZevSight({ handleExit: false, fetch: f.fetch });
    c.info('x');
    await c.flush();
    expect(f.calls).toHaveLength(0);
    vi.unstubAllEnvs();
  });

  it('normalizes service and environment to slugs', () => {
    expect(slug('Checkout Service', 'x')).toBe('checkout-service');
    expect(slug('  API.v2 ', 'x')).toBe('api.v2');
    expect(slug('!!!', 'fallback')).toBe('fallback');
  });

  it('parses Retry-After and bounds backoff', () => {
    expect(retryAfterMs('3')).toBe(3000);
    expect(retryAfterMs(null)).toBeUndefined();
    expect(retryAfterMs(new Date(Date.now() + 10_000).toUTCString())).toBeGreaterThan(8_000);
    for (let a = 0; a < 20; a++) {
      const d = backoff(a);
      expect(d).toBeGreaterThanOrEqual(0);
      expect(d).toBeLessThanOrEqual(30_000);
    }
  });
});
