# Changelog

All notable changes to Sleet are documented in this file.

## Unreleased

- Add a checksum-verified installer for prebuilt Linux and macOS CLI releases.
- Upgrade SlateDB to 0.15.0. Compaction workers now use
  `heartbeat_interval`; `heartbeat_min_interval` remains a config alias, and
  the removed byte-based heartbeat setting is no longer accepted.
- Advance each mirror target's manifest boundary after committing its head,
  allowing SlateDB 0.15 `DbReaderMode::FollowLatest` readers to cross sparse
  manifest gaps.

## 0.1.0 - 2026-07-10

Initial release.

- Coordinate garbage collection, compaction coordinators, and compaction
  workers across a stateless fleet using object storage.
- Mirror SlateDB databases to continuous and periodic targets, retain restore
  points, and restore backups into empty database roots.
- Register databases and inspect fleet placement, queue depth, and mirror lag
  through the CLI.
- Control the same operations through the asynchronous Rust `Fleet` API.
- Resolve object-store credentials and provider options from the process
  environment for fleet roots, databases, and mirror destinations.
- Publish JSON Schemas for configuration, heartbeats, and CLI responses.
