# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Docker Compose deployment with MinIO (S3-compatible object storage) and a
  Postgres-backed Iceberg SQL catalog, plus a multi-stage, non-root `Dockerfile`.
- `FLOE_*` environment variable overrides for 12-factor configuration on top of
  the YAML config, including URI-aware warehouse resolution (`s3://`, `file://`,
  and local paths).
- Passthrough of arbitrary PyIceberg catalog properties (for example S3
  endpoint, credentials, and region) from config into the catalog.
- Graceful `SIGTERM`/`SIGINT` shutdown for the long-running `floe watch` worker.
- Project governance and community health files: `LICENSE`, `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `SECURITY.md`, issue and pull request templates, and a
  Dependabot configuration.

### Changed
- Multi-upstream staleness detection now tracks the snapshot ID of every
  upstream table, so a downstream table refreshes when any upstream changes.

### Fixed
- Unpartitioned `INCREMENTAL` refresh is now idempotent (recompute and
  overwrite), eliminating duplicate rows on repeated refreshes.
- Removed an incorrect `use_ref(snapshot_id)` call and a broad `except` that
  could silently swallow read errors.

## [0.1.0] - 2026-05-05

Initial public release.

### Added
- Declarative dynamic Iceberg tables (DITs) defined in SQL with a `TARGET_LAG`
  refresh contract, inspired by Snowflake Dynamic Tables.
- SQL parser (regex + sqlglot) and a `networkx`-based DAG planner that resolves
  dependencies between tables.
- Refresh executor backed by DuckDB that reads upstream Iceberg tables and
  writes results back through PyIceberg.
- `FULL` and `INCREMENTAL` refresh modes, with partition-aware refresh windows.
- A polling watcher that auto-refreshes downstream tables when upstream Iceberg
  tables receive new commits, with a live terminal dashboard.
- `floe` command-line interface (`apply`, `watch`, `version`, and related
  commands) and a worked quickstart under `examples/`.

[Unreleased]: https://github.com/takasoft/floe/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/takasoft/floe/releases/tag/v0.1.0
