# Changelog

## 0.0.2 — 2026-05-17

- Added `SourceItem.document_hint`, a user-facing citation label that
  adapters can set independently from the technical `external_key`.
- Forward `document_hint` through IronRAG upload and replace multipart
  requests.
- Bumped the package version to 0.0.2.

## 0.0.1 — 2026-05-17

Initial release of the IronRAG connector framework.

### Framework surface

- `SourceAdapter` Protocol — implement once per vendor, with
  `SourceItemRef` (diff stage) and `SourceItem` (full payload) shapes.
  `kinds` + `primary_kinds` separate enumerated kinds from dependent
  ones (attachments/images) so the reaper never deletes a live
  dependent.
- `IronRagClient` — find-by-external-key, upload, replace, delete,
  list-by-prefix. Handles 409 duplicate-content sentinel and
  invalidates the cursor on a 404 replace.
- `BaseConnectorSettings` (pydantic-settings) — IronRAG creds, sync
  loop tuning, server bind, state path, pidfile, default policies.
  Run modes: `poll | webhook | both`.
- YAML `routing.yaml` — arbitrary fact-bag match against
  `ref.routing_facts`; per-kind `policies:` overrides for `on_new` /
  `on_changed` / `on_missing` / `on_duplicate_content`.
- `StateStore` — SQLite persistent cursor `(kind, item_id) →
  change_token`. Survives restart; framework trusts cursor over
  server-side find to dodge list-endpoint quirks.
- `Orchestrator` — per-item dispatch with in-sweep dedup (shared
  external_keys collapse to one IronRAG mutation), op-prefixed
  idempotency keys (separate upload vs replace key namespace).
- `SyncManager` — bounded-concurrency sweep, orphan reaper gated on
  clean enumeration, only walks `primary_kinds`.
- `build_app()` — FastAPI factory. `/health`, `/sync/run`,
  `/webhook/{name}` mounts with admin bearer + adapter-supplied
  extra-auth (HMAC). Webhooks register via `webhook_factory(orch)`
  so handlers share the framework-owned orchestrator.
- `seed_cursor` — one-shot bootstrap helper for libraries that
  already contain connector-owned documents.
- Structured per-item logging: `sync.item.<action>` events carry
  external_key, ironrag_document_id, library_id, title, and detail
  (the *why*).

### Reference adapter

- `examples/echo_connector/` — in-memory dict-backed SourceAdapter
  demonstrating the minimum surface a real connector implements.

### Distribution

- Dockerfile base image (`pipingspace/ironrag-connector:0.0.1`).
- 20 unit tests, ruff strict.
