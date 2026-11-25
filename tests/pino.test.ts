import { createServer, type Server } from 'node:http';
import { gunzipSync } from 'node:zlib';
import pino from 'pino';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import zevsightTransport, { mapPinoLine } from '../src/pino';
import { fromPinoLevel } from '../src/levels';

describe('fromPinoLevel', () => {
  it('matches the server table', () => {
    const table: Array<[number, string | undefined]> = [
      [0, undefined],
      [10, 'trace'],
      [15, 'trace'],
      [20, 'debug'],
      [30, 'info'],
      [35, 'info'],
      [40, 'warn'],
      [50, 'error'],
      [60, 'fatal'],
      [70, 'fatal'],
    ];
    for (const [n, want] of table) expect(fromPinoLevel(n)).toBe(want);
  });
});

describe('mapPinoLine', () => {
  it('maps a nestjs-pino request line', () => {
    const out = mapPinoLine({
      level: 30,
      time: 1795600000000,
      pid: 4242,
      hostname: 'api-7f9c',
      context: 'HTTP',
      req: { id: 7, method: 'POST', url: '/v1/pay', headers: { host: 'api' } },
      res: { statusCode: 201 },
      responseTime: 48,
      msg: 'request completed',
    });
    expect(out).toEqual({
      message: 'request completed',
      severity: 'info',
      ts: 1795600000000,
      attrs: { context: 'HTTP', 'req.id': 7, 'req.method': 'POST', 'req.url': '/v1/pay', 'res.statusCode': 201, responseTime: 48 },
      fields: { req: { headers: { host: 'api' } } },
      resource: { 'host.name': 'api-7f9c', 'process.pid': 4242 },
    });
  });

  it('keeps errors structured and uses their message when there is no msg', () => {
    const err = { type: 'TypeError', message: 'boom', stack: 'TypeError: boom\n at x' };
    const out = mapPinoLine({ level: 50, err });
    expect(out.message).toBe('boom');
    expect(out.severity).toBe('error');
    expect(out.fields).toEqual({ err });
  });

  it('maps trace ids, string levels, custom message keys and nested objects', () => {
    const out = mapPinoLine(
      { level: 'warning', message: 'hi', trace_id: 'abc', spanId: 'def', user: { id: 1 }, tags: ['a'] },
      { messageKey: 'message' },
    );
    expect(out).toMatchObject({ message: 'hi', severity: 'warn', traceId: 'abc', spanId: 'def' });
    expect(out.fields).toEqual({ user: { id: 1 }, tags: ['a'] });
    expect(mapPinoLine({ level: 'shout' }).attrs).toEqual({ level: 'shout' });
  });
});

describe('transport', () => {
  let server: Server;
  let url = '';
  const received: any[] = [];
  const headers: Record<string, string | string[] | undefined>[] = [];

  beforeAll(async () => {
    server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (c) => chunks.push(c));
      req.on('end', () => {
        let buf = Buffer.concat(chunks);
        if (req.headers['content-encoding'] === 'gzip') buf = gunzipSync(buf);
        const body = JSON.parse(buf.toString('utf8'));
        received.push(...body.events);
        headers.push(req.headers);
        res.writeHead(202, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ accepted: body.events.length }));
      });
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    const addr = server.address();
    url = `http://127.0.0.1:${typeof addr === 'object' && addr ? addr.port : 0}`;
  });
  afterAll(() => new Promise<void>((r) => server.close(() => r())));

  it('ships pino lines and flushes them when the stream ends', async () => {
    const stream = await zevsightTransport({
      apiKey: 'zsi_test',
      endpoint: url,
      service: 'checkout',
      environment: 'test',
      flushIntervalMs: 60_000, // only the close-time flush can send these
    });
    const logger = pino({ base: { pid: 1, hostname: 'h' } }, stream);
    logger.info({ orderId: 'o_1' }, 'order placed');
    logger.error(new Error('card declined'), 'payment failed');
    stream.end();
    await new Promise((r) => stream.on('close', r));

    expect(received.map((e) => e.body.message)).toEqual(['order placed', 'payment failed']);
    expect(received[0]).toMatchObject({
      service: 'checkout',
      environment: 'test',
      severity: 'info',
      attrs: { orderId: 'o_1' },
      resource: { 'host.name': 'h', 'process.pid': '1' },
    });
    expect(received[1].severity).toBe('error');
    expect(received[1].body.fields.err.message).toBe('card declined');
    expect(received[1].body.fields.err.stack).toContain('card declined');
    expect(headers[0]!.authorization).toBe('Bearer zsi_test');
  });
});
