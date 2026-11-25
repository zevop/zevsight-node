import { randomFillSync } from 'node:crypto';

/**
 * UUIDv7 (RFC 9562): a 48-bit millisecond timestamp, then random bits.
 *
 * Ids are generated here, when an event is logged, not by the server. A
 * batch that is retried therefore carries the same ids every time, and
 * ZevSight stores each event once however many times it was sent.
 *
 * Within one millisecond the 12-bit `rand_a` field is used as a counter,
 * so ids from this process sort in the order they were created.
 */
let lastMs = -1;
let counter = 0;
const rnd = new Uint8Array(10);

export function uuidv7(now: number = Date.now()): string {
  randomFillSync(rnd);
  let ms = now;
  if (ms <= lastMs) {
    ms = lastMs;
    counter++;
    if (counter > 0xfff) {
      // 4,096 ids in one millisecond: borrow the next millisecond rather
      // than lose ordering.
      ms = ++lastMs;
      counter = rnd[0]! & 0x3ff;
    }
  } else {
    lastMs = ms;
    // Start low in the range so a burst has room to count upwards.
    counter = ((rnd[0]! << 8) | rnd[1]!) & 0x3ff;
  }

  const b = new Uint8Array(16);
  // 48-bit big-endian timestamp. Split, because bit operators are 32-bit.
  const hi = Math.floor(ms / 0x100000000);
  const lo = ms >>> 0;
  b[0] = (hi >>> 8) & 0xff;
  b[1] = hi & 0xff;
  b[2] = (lo >>> 24) & 0xff;
  b[3] = (lo >>> 16) & 0xff;
  b[4] = (lo >>> 8) & 0xff;
  b[5] = lo & 0xff;
  b[6] = 0x70 | ((counter >>> 8) & 0x0f); // version 7
  b[7] = counter & 0xff;
  b[8] = 0x80 | (rnd[2]! & 0x3f); // RFC 4122 variant
  for (let i = 9; i < 16; i++) b[i] = rnd[i - 6]!;

  const h = Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
