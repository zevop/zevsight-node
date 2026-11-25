import type { Severity } from './types';

/**
 * pino's numeric levels (10 trace … 60 fatal) to ZevSight severities.
 * Ranges, not exact values, so custom levels in between land sensibly.
 * Same table as the server's envelope.FromPinoLevel.
 */
export function fromPinoLevel(n: number): Severity | undefined {
  if (!Number.isFinite(n) || n <= 0) return undefined;
  if (n < 20) return 'trace';
  if (n < 30) return 'debug';
  if (n < 40) return 'info';
  if (n < 50) return 'warn';
  if (n < 60) return 'error';
  return 'fatal';
}

/**
 * Level names loggers actually emit. Anything else returns undefined: the
 * server refuses an event with an unknown severity, so an unknown name is
 * kept as an attribute instead of costing the whole line.
 */
export function parseSeverity(name: string): Severity | undefined {
  switch (name.trim().toLowerCase()) {
    case 'trace':
    case 'verbose':
    case 'silly':
      return 'trace';
    case 'debug':
    case 'dbg':
      return 'debug';
    case 'info':
    case 'information':
    case 'informational':
    case 'notice':
    case 'log':
      return 'info';
    case 'warn':
    case 'warning':
      return 'warn';
    case 'error':
    case 'err':
      return 'error';
    case 'fatal':
    case 'critical':
    case 'crit':
    case 'alert':
    case 'emergency':
    case 'emerg':
    case 'panic':
      return 'fatal';
    default:
      return undefined;
  }
}
