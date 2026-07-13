# Changelog

## 0.1.0 — 2026-07-14

- Replaced UUID routing targets with canonical, human-readable
  `<workspace-slug>/<library-slug>` catalog references shared with IronRAG MCP.
  The redundant `workspace` key and UUID-only `library` values are no longer
  accepted.
- Resolve the complete routing snapshot through IronRAG's permission-filtered
  workspace and library catalog before startup, while keeping UUIDs internal to
  content mutations, cursor state, and orphan reaping.
- Made routing hot reload asynchronous and atomic: every target in a candidate
  file must resolve before routes and policies are swapped; invalid,
  unauthorized, or temporarily unavailable candidates leave the previous
  working snapshot active and are retried.
- Added startup cleanup and cursor-seeder catalog resolution, strict rejection
  of unknown routing keys, catalog response validation, and regression coverage
  for startup failure, authorization errors, ambiguity, cross-workspace routes,
  retry-after-failure, and friendly-ref seeding.
- Expanded `routing.yaml.example` into the complete schema reference, including
  every key, type, default, match rule, and policy enum.
- Raised dependency floors to patched `pydantic-settings`, `python-multipart`,
  and `starlette` releases after a release-gate vulnerability audit.
- Bumped the package version to 0.1.0.

## 0.0.11 — 2026-06-18

- Added routing config reload for sweeps and webhooks. When `routing.yaml`
  changes, the framework reloads the existing router and policy table in place
  before the next sync run or webhook mutation, so route and per-kind policy
  updates converge without restarting the connector process.
- Invalid or temporarily unavailable routing config keeps the previous valid
  routing active and logs `routing.reload_error` instead of breaking an
  otherwise healthy connector loop.
- Added regression coverage for successful routing/policy reload, failed reload
  keep-old behavior, and sync-manager reload invocation before a sweep.
- Bumped the package version to 0.0.11.

## 0.0.10 — 2026-06-18

- Added `IRONRAG_MUTATION_TIMEOUT_SECONDS` for upload/replace/delete admission
  requests. When unset, the framework derives a bounded timeout from the
  generic HTTP timeout and `SYNC_ITEM_TIMEOUT_SECONDS`, keeping IronRAG write
  admissions inside the per-item budget.
- Added `ironrag.mutation.start`, `ironrag.mutation.accepted`, and
  `ironrag.mutation.timeout` structured logs so operators can distinguish slow
  IronRAG mutation admission from source fetch or routing work.
- Upload/replace admission timeouts now defer the source version for a later
  sweep without advancing the cursor. This keeps full sweeps moving and avoids
  turning a slow backend mutation admission into an indefinite connector lock.
- Deferred outcomes are marked structurally with `deferred=True`, so callers do
  not have to parse human-readable detail text to recognize retry-later cases.
- Dependents are not pushed in the same item window when the primary document
  write is deferred, preventing dependent writes from extending an already
  deferred source item.
- If a dependent write is deferred after the primary document was accepted, the
  primary cursor is restored to its previous source version so a later sweep
  refetches the parent and retries the dependent instead of losing it behind a
  parent `noop_unchanged`.
- Sync reports and outcome logs now include a `deferred` count/flag for
  retry-later mutation outcomes.
- Added `REAPER_LIST_TIMEOUT_SECONDS` so a slow post-sweep IronRAG prefix-list
  request cannot hold the single-flight sync lock after source enumeration has
  already completed.
- Added regression coverage for client-side mutation timeout classification and
  replace deferral without cursor advancement, including dependent suppression
  while the primary write is deferred and dependent retry after parent cursor
  restoration, plus reaper list timeout release.
- Bumped the package version to 0.0.10.

## 0.0.9 — 2026-06-18

- Added `SYNC_ITEM_TIMEOUT_SECONDS` to bound one source ref's fetch/push work,
  including dependents. A stuck source item is cancelled, counted as an item
  error, and the sweep can continue to `sync.done` instead of holding the
  process-level sweep lock indefinitely.
- Added `sync.item.start` and `sync.item_timeout` structured logs so operators
  can see which source item is currently in flight and where a timeout happened.
- Added regression coverage that a timed-out item cancels its task, records an
  error, and releases the single-flight run lock for later sweeps.
- Bumped the package version to 0.0.9.

## 0.0.8 — 2026-06-18

- Added process-local single-flight protection for full sync sweeps. Manual
  `/sync/run`, startup, and periodic triggers no longer run overlapping
  `SyncManager.run_once()` calls against the same cursor database and IronRAG
  target.
- Manual `/sync/run` now returns HTTP 409 when a sweep is already active, while
  periodic/startup triggers log a skipped event instead of starting a second
  mutation pass.
- Added cancellation cleanup for active item tasks and a structured
  `sync.cancelled` log event when a manual request is cancelled before the
  sweep finishes.
- Added regression coverage for overlapping sweep rejection and item-task
  cancellation.
- Bumped the package version to 0.0.8.

## 0.0.7 — 2026-06-18

- Added `CURSOR_LIBRARY_LOOKUP_MAX_ROWS_PER_SWEEP` to cap best-effort legacy
  cursor library backfill before source enumeration. Large old cursor databases
  no longer spend minutes timing out document-detail lookups before they start
  processing source refs.
- Optimized legacy cursor handling for the common same-route case: if a row has
  a document id but no stored library id, the orchestrator first checks the
  currently resolved target library by external key. When the document is found
  there, it backfills `ironrag_library_id` without calling document detail.
- Backfill now updates only discovered document ownership. It does not advance
  the stored `change_token` until upload, replace, or an intentional skip
  policy has actually handled the current source version.
- Kept duplicate safety unchanged: if the current target does not contain the
  document and ownership still cannot be proven, uploads remain blocked to avoid
  creating possible duplicates.
- Bumped the package version to 0.0.7.

## 0.0.6 — 2026-06-18

- Made legacy cursor library backfill bounded and non-fatal. A sweep now
  resolves old cursor rows with a short per-document timeout and limited
  concurrency instead of letting a single slow document-detail request block
  `/sync/run` before source enumeration starts.
- Added `CURSOR_LIBRARY_LOOKUP_TIMEOUT_SECONDS` so operators can tune that
  metadata backfill independently from large upload/replace request timeouts.
- Defined the degraded reaper contract for unresolved legacy cursor rows:
  lookup failures skip destructive cleanup for unknown historical libraries in
  that sweep, while per-item orchestration still refuses to upload a possible
  duplicate until ownership is proven or the target-library lookup succeeds.
- Added regression coverage for cursor lookup timeouts before enumeration,
  partial reaper scans, duplicate-prevention on unresolved legacy cursors, and
  the existing fast route-move cleanup path.
- Bumped the package version to 0.0.6.

## 0.0.5 — 2026-06-18

- Fixed orphan reaping against cursor-paginated IronRAG document lists.
  `IronRagClient.list_documents_by_external_key_prefix` now follows
  `nextCursor` / `next_cursor` pages before returning the prefix set that
  `SyncManager` compares with the latest clean source enumeration. When a
  connector rule changes, or a source item disappears, documents outside the
  new `iter_items()` result are now considered by the reaper across the full
  library rather than only the first page.
- Cursor rows now remember the IronRAG library that owns their document id.
  The orchestrator trusts a cached document id only when it belongs to the
  currently resolved route, and the reaper also scans previously routed
  libraries from the cursor snapshot. This lets routing-target changes move a
  still-existing item to its new library and delete the old target document
  during the same clean sweep.
- Preserved the existing fallback paths: if `externalKeyPrefix` is rejected
  by the backend, the client scans pages without the server-side prefix and
  filters client-side; legacy `total` + `offset` pagination still works.
- Existing SQLite cursor databases are migrated in place with the new nullable
  `ironrag_library_id` column. Legacy rows that already have an
  `ironrag_document_id` are backfilled from IronRAG document detail before the
  cursor is trusted; if the owning library cannot be proven, the connector
  refuses to upload a possible duplicate and reports the item visibly.
- Deferred replace attempts when IronRAG reports a document is still processing
  a previous mutation. The connector now leaves the cursor on the previous
  `change_token` and retries on the next sweep instead of failing the item and
  leaving the whole sync report red for a transient backend state.
- Added the package `py.typed` marker so downstream connectors can type-check
  against `ironrag_connector` without treating the SDK as `Any`.
- Bumped the package version to 0.0.5.

## 0.0.4 — 2026-06-10

- Dependents (a page's attachments and inline images, emitted via
  `SourceItem.dependents`) are now auto-linked to their source item. The
  orchestrator threads the parent's `external_key` down to each dependent's
  upload as the new `parent_external_key` multipart field; primary items
  upload with no parent and stay `document_role=primary`. The link is
  injected once at `Orchestrator.push_ref`, so every connector inherits
  correct parentage with no adapter change.
- `IronRagClient.upload_document` gained an optional `parent_external_key`
  parameter, forwarded as the `parent_external_key` multipart form field
  only when set. IronRAG resolves it to the canonical parent and derives
  `document_role` (`attached_context` for image media, `attachment`
  otherwise) — the connector sends no role itself.
- Bumped the package version to 0.0.4.

## 0.0.3 — 2026-05-29

- `Orchestrator` default idempotency keys are now derived from a SHA-256
  of the payload bytes (plus operation and item identity) instead of the
  source `change_token`. A payload that is not byte-stable when
  re-rendered for the same logical version no longer collides with a
  stuck prior attempt (`409 idempotency_conflict`); identical retries
  still dedupe into a single mutation.
- `IronRagClient.find_document_by_external_key` resolves a document in a
  single request via the list endpoint's `search` filter, comparing the
  external key exactly client-side, and falls back to full pagination
  when the backend does not support the filter. Replaces the per-lookup
  full-library scan that dominated request volume on large libraries.
- `Orchestrator.push_ref` persists the discovered document id into the
  cursor on the unchanged path, so subsequent sweeps short-circuit via
  the cursor with no list-endpoint calls.
- Bumped the package version to 0.0.3.

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
