import { gzip as gzipCb } from 'node:zlib';
import { promisify } from 'node:util';
import { parseSeverity } from './levels';
import { uuidv7 } from './uuidv7';
import { VERSION } from './version';
import type {
  LogInput,
  QuotaState,
  RejectedEvent,
  Severity,
  WireEvent,
  ZevSightError,
  ZevSightOptions,
} from './types';

const gzip = promisify(gzipCb);

/** The server's hard limit per request (engine ADR 0001). */
const SERVER_MAX_EVENTS = 5000;
/** Bodies smaller than this are sent uncompressed: gzip costs more than it saves. */
const GZIP_MIN_BYTES = 1024;
/** Reported on the first event of the next batch after events were dropped. */
export const DROPPED_ATTR = 'zevsight.dropped_events';

const SLUG = /^[a-z0-9][a-z0-9._-]{0,63}$/;

type Queued = { ev: WireEvent; bytes: number };
type Batch = { key: string; events: Queued[] };

/** Outcome of one HTTP attempt, decided from the status alone. */
type Attempt =
  | { kind: 'done' }
  | { kind: 'retry'; afterMs?: number; status?: number; code?: string; message: string }
  | { kind: 'split' }
  | { kind: 'fail'; status?: number; code?: string; message: string; endpoint?: string };

/**
 * Sends logs to ZevSight.
 *
 * Logging never blocks and never throws: `log()` puts the line in memory
 * and returns. A background loop sends batches (every second, or sooner
 * when 500 lines or 1 MiB are waiting), one request at a time so lines
 * arrive in order. While ZevSight is unreachable the client keeps retrying
 * with backoff and holds up to `maxBufferEvents` lines; beyond that it
 * drops the oldest and says so, rather than growing without bound inside
 * your process.
 *
 * Every line gets its id (UUIDv7) when it is logged, and every batch its
 * Idempotency-Key when it is formed. A retried batch is byte-for-byte the
 * same request, so ZevSight stores each line exactly once.
 */
export class ZevSight {
  readonly endpoint: string;
  readonly service: string;
  readonly environment: string;

  private readonly apiKey: string;
  private readonly resource: Record<string, string> | undefined;
  private readonly opts: Required<
    Pick<
      ZevSightOptions,
      | 'flushIntervalMs'
      | 'maxBatchEvents'
      | 'maxBatchBytes'
      | 'maxBufferEvents'
      | 'maxRetries'
      | 'maxRetryAfterMs'
      | 'requestTimeoutMs'
      | 'shutdownTimeoutMs'
    >
  >;
  private readonly fetchImpl: typeof fetch;
  private readonly onError?: (e: ZevSightError) => void;
  private readonly onQuota?: (q: QuotaState) => void;

  private queue: Queued[] = [];
  private queueBytes = 0;
  private droppedPending = 0;
  private draining: Promise<void> | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private deadline: number | null = null;
  private wake: (() => void) | null = null;
  private closed = false;
  private disabled = false;
  private lastWarnAt = new Map<string, number>();

  /** The last quota state the server reported, if any. */
  quota: QuotaState | null = null;

  constructor(options: ZevSightOptions = {}) {
    const env = typeof process !== 'undefined' ? process.env : {};
    this.apiKey = (options.apiKey ?? env.ZEVSIGHT_INGEST_KEY ?? '').trim();
    this.service = slug(options.service ?? env.ZEVSIGHT_SERVICE ?? env.OTEL_SERVICE_NAME, 'unknown');
    this.environment = slug(
      options.environment ?? env.ZEVSIGHT_ENVIRONMENT ?? env.NODE_ENV,
      'production',
    );
    this.endpoint = resolveEndpoint(options.endpoint ?? env.ZEVSIGHT_ENDPOINT, options.region ?? env.ZEVSIGHT_REGION);
    this.resource = options.resource ? stringMap(options.resource) : undefined;
    this.opts = {
      flushIntervalMs: options.flushIntervalMs ?? 1000,
      maxBatchEvents: Math.min(Math.max(options.maxBatchEvents ?? 500, 1), SERVER_MAX_EVENTS),
      maxBatchBytes: options.maxBatchBytes ?? 1 << 20,
      maxBufferEvents: Math.max(options.maxBufferEvents ?? 20_000, 1),
      maxRetries: options.maxRetries ?? 10,
      maxRetryAfterMs: options.maxRetryAfterMs ?? 5 * 60_000,
      requestTimeoutMs: options.requestTimeoutMs ?? 15_000,
      shutdownTimeoutMs: options.shutdownTimeoutMs ?? 5_000,
    };
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    this.onError = options.onError;
    this.onQuota = options.onQuota;

    if (!this.apiKey) {
      // Like a blank log token in every Zev product: logging carries on
      // locally and nothing is sent. Said once, loudly.
      this.disabled = true;
      warn('No ingest key (ZEVSIGHT_INGEST_KEY). Logs will not be sent to ZevSight.');
      return;
    }
    if (typeof this.fetchImpl !== 'function') {
      this.disabled = true;
      warn('No fetch implementation (Node 18 or newer is required). Logs will not be sent.');
      return;
    }

    this.timer = setInterval(() => {
      if (this.queue.length > 0) this.kick();
    }, this.opts.flushIntervalMs);
    this.timer.unref?.();
    if (options.handleExit ?? true) registerExitHooks(this);
  }

  /** Lines waiting to be sent. */
  get pending(): number {
    return this.queue.length;
  }

  log(input: LogInput): void {
    if (this.disabled || this.closed) return;
    let ev: WireEvent;
    try {
      ev = this.toWire(input);
    } catch (err) {
      this.warnOnce('encode', `A log line could not be encoded and was skipped: ${(err as Error).message}`);
      return;
    }
    const bytes = Buffer.byteLength(JSON.stringify(ev));
    this.queue.push({ ev, bytes });
    this.queueBytes += bytes;

    while (this.queue.length > this.opts.maxBufferEvents) {
      const old = this.queue.shift()!;
      this.queueBytes -= old.bytes;
      this.droppedPending++;
    }
    if (this.queue.length >= this.opts.maxBatchEvents || this.queueBytes >= this.opts.maxBatchBytes) {
      this.kick();
    }
  }

  trace(message: string, attrs?: Record<string, unknown>) {
    this.log({ message, severity: 'trace', attrs });
  }
  debug(message: string, attrs?: Record<string, unknown>) {
    this.log({ message, severity: 'debug', attrs });
  }
  info(message: string, attrs?: Record<string, unknown>) {
    this.log({ message, severity: 'info', attrs });
  }
  warn(message: string, attrs?: Record<string, unknown>) {
    this.log({ message, severity: 'warn', attrs });
  }
  error(message: string, attrs?: Record<string, unknown>) {
    this.log({ message, severity: 'error', attrs });
  }
  fatal(message: string, attrs?: Record<string, unknown>) {
    this.log({ message, severity: 'fatal', attrs });
  }

  /** Sends everything logged so far. Resolves when it is delivered or given up. */
  async flush(): Promise<void> {
    if (this.disabled) return;
    while (this.queue.length > 0 || this.draining) {
      this.kick();
      await this.draining;
    }
  }

  /**
   * Flushes, then stops. Waits at most `timeoutMs` (default
   * `shutdownTimeoutMs`); whatever is still unsent then is reported through
   * onError and dropped. Safe to call more than once.
   */
  async close(timeoutMs = this.opts.shutdownTimeoutMs): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    unregisterExitHooks(this);
    if (this.disabled) return;
    this.closed = true;
    this.deadline = Math.min(this.deadline ?? Infinity, Date.now() + timeoutMs);
    this.wake?.();
    let timeout: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      this.flush(),
      new Promise<void>((resolve) => {
        timeout = setTimeout(resolve, timeoutMs + 50);
        timeout.unref?.();
      }),
    ]);
    clearTimeout(timeout);
    const left = this.queue.length;
    if (left > 0) {
      this.queue = [];
      this.queueBytes = 0;
      this.report({
        kind: 'delivery_failed',
        message: `Shutdown timed out with ${left} log lines unsent.`,
        events: left,
      });
    }
  }

  // ---------------------------------------------------------------------

  private kick(): void {
    if (this.draining || this.disabled) return;
    this.draining = this.drain().finally(() => {
      this.draining = null;
    });
  }

  private async drain(): Promise<void> {
    while (this.queue.length > 0) {
      const batch = this.takeBatch();
      await this.deliver(batch);
    }
  }

  private takeBatch(): Batch {
    const events: Queued[] = [];
    let bytes = 0;
    while (
      this.queue.length > 0 &&
      events.length < this.opts.maxBatchEvents &&
      (events.length === 0 || bytes + this.queue[0]!.bytes <= this.opts.maxBatchBytes)
    ) {
      const q = this.queue.shift()!;
      this.queueBytes -= q.bytes;
      bytes += q.bytes;
      events.push(q);
    }
    if (this.droppedPending > 0 && events.length > 0) {
      const n = this.droppedPending;
      this.droppedPending = 0;
      const first = events[0]!.ev;
      first.attrs = { ...first.attrs, [DROPPED_ATTR]: String(n) };
      this.report({ kind: 'dropped', events: n });
    }
    return { key: `zsn-${uuidv7()}`, events };
  }

  private async deliver(batch: Batch): Promise<void> {
    for (let attempt = 0; ; attempt++) {
      const result = await this.send(batch);
      switch (result.kind) {
        case 'done':
          return;
        case 'split': {
          if (batch.events.length < 2) {
            this.report({
              kind: 'delivery_failed',
              status: 413,
              code: 'PAYLOAD_TOO_LARGE',
              message: 'A single log line is larger than ZevSight accepts.',
              events: 1,
            });
            return;
          }
          const mid = Math.ceil(batch.events.length / 2);
          await this.deliver({ key: `${batch.key}.1`, events: batch.events.slice(0, mid) });
          await this.deliver({ key: `${batch.key}.2`, events: batch.events.slice(mid) });
          return;
        }
        case 'fail':
          this.report({
            kind: 'delivery_failed',
            status: result.status,
            code: result.code,
            message: result.message,
            endpoint: result.endpoint,
            events: batch.events.length,
          });
          return;
        case 'retry': {
          if (result.afterMs !== undefined && result.afterMs > this.opts.maxRetryAfterMs) {
            this.report({
              kind: 'delivery_failed',
              status: result.status,
              code: result.code,
              message: `${result.message} (asked to retry in ${Math.round(result.afterMs / 1000)} s; gave up)`,
              events: batch.events.length,
            });
            return;
          }
          if (attempt >= this.opts.maxRetries) {
            this.report({
              kind: 'delivery_failed',
              status: result.status,
              code: result.code,
              message: `${result.message} (gave up after ${attempt + 1} attempts)`,
              events: batch.events.length,
            });
            return;
          }
          const delay = result.afterMs ?? backoff(attempt);
          if (!(await this.sleep(delay))) {
            this.report({
              kind: 'delivery_failed',
              status: result.status,
              code: result.code,
              message: `${result.message} (shutdown deadline reached while retrying)`,
              events: batch.events.length,
            });
            return;
          }
        }
      }
    }
  }

  /** Waits, unless that would pass the shutdown deadline. False means "stop". */
  private sleep(ms: number): Promise<boolean> {
    if (this.deadline !== null && Date.now() + ms > this.deadline) return Promise.resolve(false);
    return new Promise((resolve) => {
      const t = setTimeout(() => {
        this.wake = null;
        resolve(true);
      }, ms);
      // close() wakes a long retry wait: try once more now if there is
      // still a little time before the deadline, otherwise stop.
      this.wake = () => {
        clearTimeout(t);
        this.wake = null;
        resolve(this.deadline !== null && this.deadline - Date.now() > 250);
      };
    });
  }

  private async send(batch: Batch): Promise<Attempt> {
    const json = JSON.stringify({ events: batch.events.map((q) => q.ev) });
    const headers: Record<string, string> = {
      authorization: `Bearer ${this.apiKey}`,
      'content-type': 'application/json',
      'idempotency-key': batch.key,
      'user-agent': `zevsight-node/${VERSION}`,
    };
    let body: BodyInit = json;
    if (json.length >= GZIP_MIN_BYTES) {
      // A Buffer is a valid fetch body at runtime; TypeScript's DOM types
      // only disagree about its ArrayBuffer generic.
      body = (await gzip(json)) as unknown as BodyInit;
      headers['content-encoding'] = 'gzip';
    }

    let res: Response;
    try {
      res = await this.fetchImpl(`${this.endpoint}/v1/ingest/events`, {
        method: 'POST',
        headers,
        body,
        signal: AbortSignal.timeout(this.opts.requestTimeoutMs),
      });
    } catch (err) {
      return { kind: 'retry', message: `Could not reach ZevSight: ${(err as Error).message}` };
    }

    const status = res.status;
    const parsed = await readJson(res);
    const code = typeof parsed?.code === 'string' ? parsed.code : undefined;
    const message = typeof parsed?.message === 'string' ? parsed.message : `HTTP ${status}`;

    if (status === 202 || status === 200) {
      this.handleRejected(batch, parsed?.rejected);
      if (parsed?.quota && typeof parsed.quota.used_ratio === 'number') {
        this.quota = { usedRatio: parsed.quota.used_ratio, state: parsed.quota.state };
        try {
          this.onQuota?.(this.quota);
        } catch {
          // A hook must never break logging.
        }
      }
      return { kind: 'done' };
    }
    if (status === 400 && Array.isArray(parsed?.rejected)) {
      // Every event was refused for a reason a retry cannot fix.
      this.handleRejected(batch, parsed.rejected);
      return { kind: 'done' };
    }
    if (status === 413) return { kind: 'split' };
    if (status === 408 || status === 429 || status >= 500) {
      return { kind: 'retry', afterMs: retryAfterMs(res.headers.get('retry-after')), status, code, message };
    }
    const endpoint = typeof parsed?.endpoint === 'string' ? parsed.endpoint : undefined;
    return { kind: 'fail', status, code, message, endpoint };
  }

  private handleRejected(batch: Batch, rejected: unknown): void {
    if (!Array.isArray(rejected) || rejected.length === 0) return;
    const out: RejectedEvent[] = [];
    for (const r of rejected as Array<{ index?: number; field?: string; message?: string }>) {
      const q = typeof r.index === 'number' ? batch.events[r.index] : undefined;
      if (!q) continue;
      out.push({ field: r.field, message: r.message ?? 'Refused', event: q.ev });
    }
    if (out.length > 0) this.report({ kind: 'rejected', rejected: out });
  }

  private report(e: ZevSightError): void {
    if (this.onError) {
      try {
        this.onError(e);
      } catch {
        // A hook must never break logging.
      }
      return;
    }
    const text =
      e.kind === 'rejected'
        ? `${e.rejected.length} log line(s) refused: ${e.rejected[0]!.message}`
        : e.kind === 'dropped'
          ? `${e.events} log line(s) dropped because the buffer was full`
          : `${e.events} log line(s) not delivered: ${e.message}`;
    this.warnOnce(e.kind, text);
  }

  /** At most one console line per kind per minute, so a broken link cannot flood stderr. */
  private warnOnce(kind: string, text: string): void {
    const now = Date.now();
    if ((this.lastWarnAt.get(kind) ?? 0) + 60_000 > now) return;
    this.lastWarnAt.set(kind, now);
    warn(text);
  }

  private toWire(input: LogInput): WireEvent {
    const ev: WireEvent = {
      id: uuidv7(),
      type: 'log',
      service: input.service ? slug(input.service, this.service) : this.service,
      environment: input.environment ? slug(input.environment, this.environment) : this.environment,
      ts: toIso(input.ts),
      body: { message: typeof input.message === 'string' ? input.message : String(input.message ?? '') },
    };
    let attrs = input.attrs ? stringMap(input.attrs) : undefined;
    if (input.severity) {
      const sev = parseSeverity(input.severity) as Severity | undefined;
      if (sev) ev.severity = sev;
      else attrs = { ...attrs, level: String(input.severity) };
    }
    const trace = hexId(input.traceId, 32);
    if (trace) ev.trace_id = trace;
    else if (input.traceId) attrs = { ...attrs, trace_id: String(input.traceId) };
    const span = hexId(input.spanId, 16);
    if (span) ev.span_id = span;
    else if (input.spanId) attrs = { ...attrs, span_id: String(input.spanId) };
    if (attrs && Object.keys(attrs).length > 0) ev.attrs = attrs;
    const resource = input.resource ? { ...this.resource, ...stringMap(input.resource) } : this.resource;
    if (resource && Object.keys(resource).length > 0) ev.resource = resource;
    if (input.fields && Object.keys(input.fields).length > 0) ev.body.fields = input.fields;
    return ev;
  }
}

// ---------------------------------------------------------------------------

function warn(text: string): void {
  // stderr, not a logger: the logger may be the thing that is broken.
  process.stderr?.write?.(`[zevsight] ${text}\n`);
}

/** Exponential backoff with full jitter: 0.5 s, 1 s, 2 s … capped at 30 s. */
export function backoff(attempt: number): number {
  const cap = Math.min(30_000, 500 * 2 ** attempt);
  return Math.floor(cap / 2 + Math.random() * (cap / 2));
}

/** Retry-After as seconds or an HTTP date, in ms. */
export function retryAfterMs(header: string | null): number | undefined {
  if (!header) return undefined;
  const secs = Number(header);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const at = Date.parse(header);
  return Number.isNaN(at) ? undefined : Math.max(0, at - Date.now());
}

async function readJson(res: Response): Promise<any> {
  try {
    const text = await res.text();
    return text ? JSON.parse(text) : undefined;
  } catch {
    return undefined;
  }
}

function resolveEndpoint(endpoint: string | undefined, region: string | undefined): string {
  if (endpoint && endpoint.trim()) return endpoint.trim().replace(/\/+$/, '');
  const r = (region ?? 'eu').trim().toLowerCase();
  // The EU is the default region and keeps the plain hostname forever.
  return r === '' || r === 'eu' ? 'https://ingest.zevsight.com' : `https://${r}.ingest.zevsight.com`;
}

/** ZevSight wants lower-case slugs; turn "Checkout Service" into "checkout-service". */
export function slug(value: string | undefined, fallback: string): string {
  if (!value) return fallback;
  const s = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '-')
    .replace(/^[^a-z0-9]+/, '')
    .slice(0, 64);
  return SLUG.test(s) ? s : fallback;
}

function toIso(ts: LogInput['ts']): string {
  if (ts === undefined) return new Date().toISOString();
  const d = ts instanceof Date ? ts : new Date(ts);
  return Number.isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
}

function hexId(v: string | undefined, len: number): string | undefined {
  if (!v) return undefined;
  const s = String(v).toLowerCase();
  return s.length === len && /^[0-9a-f]+$/.test(s) && /[1-9a-f]/.test(s) ? s : undefined;
}

/** Attribute values are strings on the wire. */
export function stringMap(m: Record<string, unknown>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(m)) {
    if (v === undefined || v === null) continue;
    out[k] = attrString(v);
  }
  return out;
}

function attrString(v: unknown): string {
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean' || typeof v === 'bigint') return String(v);
  if (v instanceof Date) return v.toISOString();
  try {
    return JSON.stringify(v) ?? String(v);
  } catch {
    return String(v);
  }
}

// --- process exit ------------------------------------------------------------

const clients = new Set<ZevSight>();
let hooked = false;

function registerExitHooks(c: ZevSight): void {
  clients.add(c);
  if (hooked || typeof process === 'undefined' || typeof process.on !== 'function') return;
  hooked = true;
  process.on('beforeExit', onBeforeExit);
  process.on('SIGTERM', onSignal);
  process.on('SIGINT', onSignal);
}

function unregisterExitHooks(c: ZevSight): void {
  clients.delete(c);
  if (clients.size === 0 && hooked) {
    hooked = false;
    process.off('beforeExit', onBeforeExit);
    process.off('SIGTERM', onSignal);
    process.off('SIGINT', onSignal);
  }
}

function onBeforeExit(): void {
  // beforeExit fires again once this work finishes, so this naturally loops
  // until nothing is left, then the process exits.
  for (const c of clients) if (c.pending > 0) void c.flush();
}

let signalled = false;
function onSignal(signal: NodeJS.Signals): void {
  if (signalled) return;
  signalled = true;
  const all = [...clients];
  void Promise.all(all.map((c) => c.close())).finally(() => {
    // We took over the signal by listening to it. If nobody else is
    // listening, do what Node would have done: exit by that signal.
    if (process.listenerCount(signal) === 0) process.kill(process.pid, signal);
  });
}
