# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-25

### Added

- `ZevSight` client: batched, gzipped delivery to the ingest API with
  client-side UUIDv7 ids and per-batch Idempotency-Key (exactly-once storage
  across retries)
- Retries with jittered exponential backoff; honours `Retry-After`; splits a
  batch on 413; bounded buffer that drops the oldest lines and reports it
- `onError` for refused lines, failed batches and dropped lines; `onQuota`
- Flush on `beforeExit`, `SIGTERM` and `SIGINT`, and `close()` with a deadline
- pino transport at `@zevsight/node/pino`
- `heartbeat()` and `withHeartbeat()` for cron jobs and workers
- ESM + CJS builds with TypeScript types
