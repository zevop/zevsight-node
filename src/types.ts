export type Severity = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';

/** One log line as you hand it to the client. */
export interface LogInput {
  message: string;
  severity?: Severity;
  /** When it happened. Defaults to now. */
  ts?: Date | string | number;
  /** Overrides the client's service for this line. */
  service?: string;
  /** Overrides the client's environment for this line. */
  environment?: string;
  /** Flat, searchable key/values. Non-string values are converted to strings. */
  attrs?: Record<string, unknown>;
  /** Structured data that is not a flat attribute (nested objects, arrays). */
  fields?: Record<string, unknown>;
  /** Overrides or extends the client's resource for this line. */
  resource?: Record<string, unknown>;
  traceId?: string;
  spanId?: string;
}

/** The wire shape of POST /v1/ingest/events. */
export interface WireEvent {
  id: string;
  type: 'log';
  service: string;
  environment: string;
  ts: string;
  severity?: Severity;
  trace_id?: string;
  span_id?: string;
  attrs?: Record<string, string>;
  resource?: Record<string, string>;
  body: { message: string; fields?: Record<string, unknown> };
}

/** An event the server refused, with the reason it gave. */
export interface RejectedEvent {
  field?: string;
  message: string;
  event: WireEvent;
}

export type ZevSightError =
  | {
      /** Some events in a batch were refused (the rest were stored). */
      kind: 'rejected';
      rejected: RejectedEvent[];
    }
  | {
      /** A whole batch could not be delivered and was given up. */
      kind: 'delivery_failed';
      status?: number;
      code?: string;
      message: string;
      /** Present when the key belongs to another region (HTTP 421). */
      endpoint?: string;
      events: number;
    }
  | {
      /** The buffer was full and the oldest events were dropped. */
      kind: 'dropped';
      events: number;
    };

export interface QuotaState {
  usedRatio: number;
  state: 'warning' | 'over' | 'warn_and_above_only';
}

export interface ZevSightOptions {
  /** An ingest key (`zsi_…`). Defaults to the ZEVSIGHT_INGEST_KEY environment variable. */
  apiKey?: string;
  /** Defaults to ZEVSIGHT_SERVICE, then OTEL_SERVICE_NAME, then "unknown". */
  service?: string;
  /** Defaults to ZEVSIGHT_ENVIRONMENT, then NODE_ENV, then "production". */
  environment?: string;
  /**
   * The data region your project lives in. Defaults to ZEVSIGHT_REGION, then
   * "eu". Ignored when `endpoint` is set.
   */
  region?: string;
  /** The ingest base URL. Defaults to ZEVSIGHT_ENDPOINT, then the region's host. */
  endpoint?: string;
  /** Attached to every event as `resource`, e.g. host, version, region. */
  resource?: Record<string, unknown>;
  /** Send at least this often, in ms. Default 1000. */
  flushIntervalMs?: number;
  /** Send when this many events are waiting. Default 500, max 5000. */
  maxBatchEvents?: number;
  /** Send when this many bytes (JSON) are waiting. Default 1 MiB. */
  maxBatchBytes?: number;
  /** Events held while the server is unreachable; the oldest are dropped beyond this. Default 20,000. */
  maxBufferEvents?: number;
  /** Retries per batch before it is given up. Default 10. */
  maxRetries?: number;
  /** A Retry-After longer than this gives the batch up instead of waiting. Default 5 minutes. */
  maxRetryAfterMs?: number;
  /** Per-request timeout. Default 15 s. */
  requestTimeoutMs?: number;
  /** Flush on process exit (beforeExit, SIGTERM, SIGINT). Default true. */
  handleExit?: boolean;
  /** How long a flush on exit or close() may take. Default 5 s. */
  shutdownTimeoutMs?: number;
  /** Called for refused events, failed batches and dropped events. Never throws into your code. */
  onError?: (error: ZevSightError) => void;
  /** Called when the project passes 80% of its monthly ingest. */
  onQuota?: (quota: QuotaState) => void;
  /** For tests and unusual runtimes. Defaults to the global fetch. */
  fetch?: typeof fetch;
}
