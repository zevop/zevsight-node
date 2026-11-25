# @zevsight/node

Send logs and heartbeats from Node.js to [ZevSight](https://zevsight.com).

- **pino transport**: one target in your pino or nestjs-pino config.
- **Client**: `zevsight.info('order placed', { orderId })` from any Node code.
- **Heartbeats**: ping a monitor from cron jobs and workers.

Logging never blocks your code and never throws into it. Lines are batched,
compressed and retried with backoff; each line gets its id when it is logged,
so a retried batch is stored exactly once.

Requires Node.js 18 or newer.

```bash
npm install @zevsight/node
```

## pino and nestjs-pino

Add ZevSight as a transport target. Keep stdout as well if you like.

```ts
transport: {
  targets: [
    { target: 'pino/file', options: { destination: 1 } },
    { target: '@zevsight/node/pino', options: { apiKey: process.env.ZEVSIGHT_INGEST_KEY } },
  ],
}
```

Moving from Better Stack (`@logtail/pino`)? Replace its target with the line
above; nothing else in your logger changes.

```diff
- { target: '@logtail/pino', options: { sourceToken: LOGTAIL_TOKEN, options: { endpoint } }, level: 'info' },
+ { target: '@zevsight/node/pino', options: { apiKey: process.env.ZEVSIGHT_INGEST_KEY }, level: 'info' },
```

With nestjs-pino this sits in `LoggerModule.forRoot({ pinoHttp: { transport } })`.

How pino lines are mapped:

| pino | ZevSight |
|---|---|
| `level` | severity (`trace` … `fatal`) |
| `time` | event time |
| `msg` | message (the error's message if there is no `msg`) |
| `err` | `fields.err` with type, message, stack |
| `hostname`, `pid` | resource `host.name`, `process.pid` |
| `req`, `res` | attributes `req.method`, `req.url`, `res.statusCode` … (nested objects such as headers go to fields) |
| `trace_id`, `span_id` | the event's trace and span ids |
| other flat values (`context`, `responseTime`) | attributes, searchable as `attrs.context:HTTP` |
| other objects and arrays | fields |

If you renamed pino's keys, pass `messageKey` or `errorKey` in the target's
options.

## Client

```ts
import { ZevSight } from '@zevsight/node';

const zevsight = new ZevSight({ service: 'billing-worker' }); // key from ZEVSIGHT_INGEST_KEY

zevsight.info('invoice sent', { invoiceId: 'inv_42', amount: 1200 });
zevsight.error('payment failed', { provider: 'zevpay' });
zevsight.log({
  message: 'checkout completed',
  severity: 'info',
  attrs: { route: '/v1/checkout' },        // flat, searchable
  fields: { cart: { items: 3 } },          // structured
  traceId: '4bf92f3577b34da6a3ce929d0e0e4736',
});

await zevsight.close(); // flush before a short-lived script exits
```

## OpenTelemetry

Already using an OpenTelemetry SDK or Collector? Point its OTLP/HTTP logs
exporter at ZevSight instead; you do not need this package.

```bash
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://ingest.zevsight.com/v1/ingest/otlp/logs
OTEL_EXPORTER_OTLP_LOGS_HEADERS="Authorization=Bearer zsi_…"
OTEL_EXPORTER_OTLP_LOGS_PROTOCOL=http/protobuf
```

## Heartbeats

Create a heartbeat monitor in ZevSight and copy its ping URL.

```ts
import { heartbeat, withHeartbeat } from '@zevsight/node';

// Wrap a job: start, then ok, or fail with the error's message.
await withHeartbeat(process.env.ZEVSIGHT_HEARTBEAT_NIGHTLY_EXPORT!, () => runNightlyExport());

// Or ping by hand.
const hb = heartbeat(process.env.ZEVSIGHT_HEARTBEAT_QUEUE_WORKER!);
await hb.ok();
await hb.fail('disk full');
```

Pings never throw. Each resolves to `true` when ZevSight recorded it.

## Configuration

| Option | Environment variable | Default |
|---|---|---|
| `apiKey` | `ZEVSIGHT_INGEST_KEY` | none: nothing is sent, and a warning is printed once |
| `service` | `ZEVSIGHT_SERVICE`, then `OTEL_SERVICE_NAME` | `unknown` |
| `environment` | `ZEVSIGHT_ENVIRONMENT`, then `NODE_ENV` | `production` |
| `region` | `ZEVSIGHT_REGION` | `eu` (https://ingest.zevsight.com) |
| `endpoint` | `ZEVSIGHT_ENDPOINT` | the region's ingest host |
| `resource` | | none; attached to every line (version, region…) |
| `flushIntervalMs` | | `1000` |
| `maxBatchEvents` | | `500` (max 5000) |
| `maxBatchBytes` | | `1048576` |
| `maxBufferEvents` | | `20000`; beyond this the oldest lines are dropped and counted |
| `maxRetries` | | `10` |
| `maxRetryAfterMs` | | 5 minutes; a longer `Retry-After` (monthly quota reached) gives the batch up |
| `requestTimeoutMs` | | `15000` |
| `handleExit` | | `true`: flush on `beforeExit`, `SIGTERM`, `SIGINT` |
| `shutdownTimeoutMs` | | `5000` |
| `onError(e)` | | prints at most one line per minute to stderr |
| `onQuota(q)` | | none; called past 80% of the monthly quota |

Service and environment are made into slugs (`Checkout Service` becomes
`checkout-service`), because ZevSight refuses anything else.

## What happens when things go wrong

| Situation | Behaviour |
|---|---|
| ZevSight unreachable, 5xx, 408 | Retried with exponential backoff (0.5 s to 30 s, jittered) |
| 429 or 503 with `Retry-After` | Waits exactly that long, unless longer than `maxRetryAfterMs` |
| Some lines refused (too old for the project's retention, malformed) | The rest are stored; `onError` gets `{ kind: 'rejected', rejected }` with each line and the reason |
| Wrong key or wrong region | Not retried; `onError` gets `delivery_failed` (with the right `endpoint` for a region mismatch) |
| Buffer full while offline | Oldest lines dropped; `onError` gets `{ kind: 'dropped' }` and the next batch carries `zevsight.dropped_events` |

## Shutdown

- **pino transport**: pino closes the transport when your process exits,
  including after `process.exit()`, and the transport flushes what it holds
  before closing.
- **Client**: with `handleExit` (the default) the client flushes on
  `beforeExit`, `SIGTERM` and `SIGINT`, for up to `shutdownTimeoutMs`. If
  nothing else listens for the signal, it then exits the way Node would
  have. In a short-lived script, `await zevsight.close()` at the end.
- A crash that kills the process (`SIGKILL`, out of memory) loses what was
  not yet sent: at most one flush interval of lines.

## License

MIT
