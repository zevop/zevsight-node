/**
 * Against a running ZevSight dev stack (backend :3080, gateway :3081,
 * query :3082). Opt-in: ZS_INTEGRATION=1 npm run test:integration
 *
 * Creates an organization, project and ingest key through dev login, ships
 * lines through the pino transport from child processes (exactly as an app
 * would), and reads them back through the query API.
 */
import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import { ZevSight } from '../src/index';

const RUN = process.env.ZS_INTEGRATION === '1';
const BACKEND = process.env.ZS_BACKEND_URL ?? 'http://localhost:3080';
const GATEWAY = process.env.ZS_GATEWAY_URL ?? 'http://localhost:3081';
const QUERY = process.env.ZS_QUERY_URL ?? 'http://localhost:3082';
const TRANSPORT = resolve(__dirname, '../dist/pino.js');

async function setup() {
  let cookie = '';
  async function gql(query: string) {
    const res = await fetch(`${BACKEND}/graphql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'apollo-require-preflight': 'true', cookie },
      body: JSON.stringify({ query }),
    });
    const set = res.headers.getSetCookie?.() ?? [];
    if (set.length) cookie = set.map((c) => c.split(';')[0]).join('; ');
    const json = (await res.json()) as any;
    if (json.errors) throw new Error(JSON.stringify(json.errors));
    return json.data;
  }
  const tag = randomBytes(4).toString('hex');
  await gql(`mutation { devLogin(input:{email:"sdk-${tag}@zevop.test"}) { id } }`);
  const org = (await gql(`mutation { createOrganization(input:{name:"SDK ${tag}", country:"NG"}) { id } }`))
    .createOrganization.id;
  const prj = (await gql(`mutation { createProject(organizationId:"${org}", input:{name:"SDK"}) { id } }`))
    .createProject.id;
  const key = (
    await gql(
      `mutation { createApiKey(organizationId:"${org}", input:{name:"sdk", kind: ingest, projectIds:["${prj}"]}) { secret } }`,
    )
  ).createApiKey.secret as string;
  const ticket = (await gql(`mutation { engineTicket(organizationId:"${org}") { token } }`)).engineTicket
    .token as string;
  return { prj, key, ticket };
}

async function search(ctx: { prj: string; ticket: string }, q: string): Promise<any[]> {
  const from = new Date(Date.now() - 15 * 60_000).toISOString();
  const url = new URL('/v1/query/logs', QUERY);
  url.searchParams.set('project', ctx.prj);
  url.searchParams.set('from', from);
  url.searchParams.set('q', q);
  url.searchParams.set('limit', '1000');
  const res = await fetch(url, { headers: { authorization: `Bearer ${ctx.ticket}` } });
  if (!res.ok) throw new Error(`query ${res.status}: ${await res.text()}`);
  return ((await res.json()) as any).events;
}

async function waitFor(ctx: { prj: string; ticket: string }, q: string, n: number, ms = 20_000) {
  const end = Date.now() + ms;
  let got: any[] = [];
  while (Date.now() < end) {
    got = await search(ctx, q);
    if (got.length >= n) break;
    await new Promise((r) => setTimeout(r, 300));
  }
  return got;
}

/** A child process using pino with the transport, the way a Zev product would. */
function runChild(key: string, marker: string, lines: number, exitHow: 'natural' | 'exit') {
  const script = `
    const pino = require('pino');
    const logger = pino({ level: 'info', transport: { targets: [
      { target: ${JSON.stringify(TRANSPORT)}, level: 'info',
        options: { apiKey: ${JSON.stringify(key)}, endpoint: ${JSON.stringify(GATEWAY)},
                   service: 'sdk-integration', environment: 'test' } } ] } });
    for (let i = 0; i < ${lines}; i++) logger.info({ i, context: 'Integration' }, '${marker} line ' + i);
    logger.error(new Error('${marker} boom'), '${marker} failed');
    ${exitHow === 'exit' ? 'process.exit(0);' : ''}
  `;
  execFileSync(process.execPath, ['-e', script], { cwd: resolve(__dirname, '..'), stdio: 'inherit', timeout: 30_000 });
}

describe.runIf(RUN)('integration: dev stack', () => {
  it('ships lines from a pino app that exits naturally, exactly once', async () => {
    const ctx = await setup();
    const marker = 'nat' + randomBytes(5).toString('hex');
    runChild(ctx.key, marker, 50, 'natural');
    const got = await waitFor(ctx, marker, 51);
    expect(got).toHaveLength(51);
    expect(new Set(got.map((e) => e.id)).size).toBe(51);
    const err = got.find((e) => e.severity === 'error');
    expect(err.body.fields.err.message).toBe(`${marker} boom`);
    expect(err.service).toBe('sdk-integration');
    const line = got.find((e) => e.body.message === `${marker} line 7`);
    expect(line.attrs).toMatchObject({ i: '7', context: 'Integration' });
    expect(line.resource['host.name']).toBeTruthy();
  }, 60_000);

  it('ships lines from a pino app that calls process.exit() right after logging', async () => {
    const ctx = await setup();
    const marker = 'ext' + randomBytes(5).toString('hex');
    runChild(ctx.key, marker, 50, 'exit');
    // pino ends its transport on exit and waits for close(), which flushes.
    const got = await waitFor(ctx, marker, 51, 10_000);
    expect(got).toHaveLength(51);
  }, 60_000);

  it('the client retries the same batch without duplicates', async () => {
    const ctx = await setup();
    const marker = 'dup' + randomBytes(5).toString('hex');
    // A fetch that delivers the request but loses the response, then lets
    // the retry through: the server sees the same batch twice.
    let calls = 0;
    const flaky = (async (url: string, init: RequestInit) => {
      const res = await fetch(url, init);
      if (++calls === 1) throw new TypeError('connection reset after send');
      return res;
    }) as unknown as typeof fetch;
    const c = new ZevSight({ apiKey: ctx.key, endpoint: GATEWAY, service: 'sdk-integration', handleExit: false, fetch: flaky });
    for (let i = 0; i < 10; i++) c.info(`${marker} ${i}`);
    await c.flush();
    await c.close();
    expect(calls).toBe(2);
    const got = await waitFor(ctx, marker, 10);
    await new Promise((r) => setTimeout(r, 1500));
    const again = await search(ctx, marker);
    expect(got).toHaveLength(10);
    expect(again).toHaveLength(10);
  }, 60_000);
});
