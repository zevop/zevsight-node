import build from 'pino-abstract-transport';
import { ZevSight } from './client';
import { fromPinoLevel, parseSeverity } from './levels';
import type { LogInput, ZevSightOptions } from './types';

/**
 * Options for the pino transport. Everything in ZevSightOptions except the
 * callbacks, which cannot cross into pino's transport worker thread.
 */
export interface PinoTransportOptions
  extends Omit<ZevSightOptions, 'onError' | 'onQuota' | 'fetch' | 'handleExit'> {
  /** pino's messageKey, if you changed it. Default "msg". */
  messageKey?: string;
  /** pino's errorKey, if you changed it. Default "err". */
  errorKey?: string;
}

/** Keys pino writes itself. They are mapped to dedicated fields, never to attrs. */
const PINO_KEYS = new Set(['level', 'time', 'v', 'hostname', 'pid']);
/** Objects flattened one level into attrs (`req.method`, `res.statusCode`). */
const FLATTEN = new Set(['req', 'res']);

/**
 * One pino line to one ZevSight log line.
 *
 * - `level` (number or name) → severity
 * - `time` → ts; `msg` → message (or the error's message when there is none)
 * - `err` → fields.err with type, message, stack and anything else on it
 * - `hostname`, `pid` → resource (`host.name`, `process.pid`)
 * - `req`, `res` → flattened into attrs (`req.method`, `res.statusCode`),
 *   anything nested deeper (headers) into fields
 * - `trace_id`/`span_id` (or camelCase) → the event's trace and span ids
 * - any other flat value (`context`, `responseTime`) → attrs; objects and
 *   arrays → fields
 */
export function mapPinoLine(
  obj: Record<string, unknown>,
  opts: { messageKey?: string; errorKey?: string } = {},
): LogInput {
  const messageKey = opts.messageKey ?? 'msg';
  const errorKey = opts.errorKey ?? 'err';
  const attrs: Record<string, unknown> = {};
  const fields: Record<string, unknown> = {};
  const resource: Record<string, unknown> = {};
  const out: LogInput = { message: '' };

  if (typeof obj.level === 'number') {
    const sev = fromPinoLevel(obj.level);
    if (sev) out.severity = sev;
  } else if (typeof obj.level === 'string') {
    const sev = parseSeverity(obj.level);
    if (sev) out.severity = sev;
    else attrs.level = obj.level;
  }
  if (typeof obj.time === 'number' || typeof obj.time === 'string') out.ts = obj.time;
  if (obj.hostname !== undefined) resource['host.name'] = obj.hostname;
  if (obj.pid !== undefined) resource['process.pid'] = obj.pid;

  for (const [key, value] of Object.entries(obj)) {
    if (PINO_KEYS.has(key) || value === undefined) continue;
    if (key === messageKey) {
      out.message = typeof value === 'string' ? value : safeJson(value);
      continue;
    }
    if (key === errorKey && value && typeof value === 'object') {
      fields.err = value;
      continue;
    }
    if (key === 'trace_id' || key === 'traceId') {
      out.traceId = String(value);
      continue;
    }
    if (key === 'span_id' || key === 'spanId') {
      out.spanId = String(value);
      continue;
    }
    if (FLATTEN.has(key) && value && typeof value === 'object' && !Array.isArray(value)) {
      for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
        if (v === undefined || v === null) continue;
        if (typeof v === 'object') {
          const nested = (fields[key] ??= {}) as Record<string, unknown>;
          nested[k] = v;
        } else attrs[`${key}.${k}`] = v;
      }
      continue;
    }
    if (value === null || typeof value !== 'object') attrs[key] = value;
    else fields[key] = value;
  }

  if (!out.message && fields.err && typeof (fields.err as { message?: unknown }).message === 'string') {
    out.message = (fields.err as { message: string }).message;
  }
  if (Object.keys(attrs).length) out.attrs = attrs;
  if (Object.keys(fields).length) out.fields = fields;
  if (Object.keys(resource).length) out.resource = resource;
  return out;
}

function safeJson(v: unknown): string {
  try {
    return JSON.stringify(v) ?? String(v);
  } catch {
    return String(v);
  }
}

/**
 * The pino transport. Use it as a target:
 *
 *   { target: '@zevsight/node/pino', options: { apiKey: process.env.ZEVSIGHT_INGEST_KEY } }
 *
 * It runs in pino's worker thread, batches lines like the client does, and
 * flushes what it holds when pino closes the transport.
 */
export default async function zevsightTransport(options: PinoTransportOptions = {}) {
  const { messageKey, errorKey, ...clientOptions } = options;
  // The worker never sees the process's signals; pino ends the stream on
  // exit and `close` below flushes.
  const client = new ZevSight({ ...clientOptions, handleExit: false });
  return build(
    async (source: AsyncIterable<Record<string, unknown>>) => {
      for await (const obj of source) client.log(mapPinoLine(obj, { messageKey, errorKey }));
    },
    {
      async close() {
        await client.close();
      },
    },
  );
}
