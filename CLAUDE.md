# IronRAG.ConnectorTemplate — agent + developer guide

This repo is a **framework for writing data-source connectors that push
content into IronRAG**. Anything that reads from a vendor wiki, file
share, ticket system, or CMS and wants to feed IronRAG is a connector;
this template gives you all the IronRAG-side plumbing and asks you to
implement exactly one Protocol.

Companion projects:

- [`IronRAG`](https://github.com/mlimarenko/IronRAG) — the RAG backend
  this connector targets. Its `/v1/content/documents/*` HTTP API is
  the canonical write surface.
- The bundled reference adapter under `examples/echo_connector/` is the
  minimal end-to-end example. A full real-world adapter would
  additionally ship its own vendor REST client, webhook verifier, and
  routing-facts taxonomy.

## Mental model

```
vendor source ─► YourSourceAdapter ─► ironrag_connector framework ─► IronRAG
                 (you write this)      (this repo, do not fork)
```

The framework owns:

- **HTTP client** against IronRAG (`upload`, `replace`, `delete`,
  `find_by_external_key`, `list_by_prefix`).
- **Routing** — YAML rules mapping arbitrary adapter-emitted facts
  (`shelf`, `tag`, `space`, …) to `(workspace_id, library_id)`.
- **Per-kind push policy** — `on_new` / `on_changed` / `on_missing` /
  `on_duplicate_content`, each independently configurable. Lets ops
  treat pages, attachments, and images differently in one connector.
- **Persistent SQLite cursor** at `STATE_DB_PATH` so restarts don't
  re-push everything; the diff stage reads
  `(kind, item_id) → change_token` from this DB.
- **Periodic sync loop** with bounded concurrency, orphan reaper gated
  on a clean enumeration.
- **FastAPI server** — `/health`, `/sync/run`, optional
  `/webhook/{name}` mounts with admin bearer + adapter-supplied
  extra-auth (HMAC, etc).
- **Pidfile lock**, **structlog JSON observability**.

The adapter owns:

- One vendor REST client.
- A `SourceAdapter` Protocol implementation: `iter_items` (yields
  lightweight refs with a change_token), `fetch` (materializes one ref
  into a full `SourceItem` with bytes + dependents), `external_key` /
  `parse_external_key` (round-trip a stable IronRAG identity),
  `close`.
- Whatever YAML facts the routing layer should match on. Anything you
  return from `ref.routing_facts` becomes a valid match key in
  `routing.yaml`.

Adapters use `external_key` as the technical sync identity: it must stay
stable so the connector can find, replace, and reap the right IronRAG
document. A `SourceItem` may also set `document_hint` to a canonical URL
or any other user-facing label that IronRAG can surface to MCP agents in
citations; it is separate from `external_key` and does not participate
in sync identity.

## File map

```
src/ironrag_connector/
  source.py        — Protocol + data shapes (read this first)
  ironrag.py       — IronRAG HTTP client
  config.py        — BaseConnectorSettings + PolicyDefaults
  routing.py       — Router + YAML schema + PolicyOverrides
  policy.py        — UpsertAction / UpdateAction / DeleteAction enums
  state.py         — SQLite cursor (thread-safe, WAL)
  orchestrator.py  — single-item decision + push (cursor > find > upload/replace)
  sync.py          — periodic sweep + reaper, bounded concurrency
  server.py        — FastAPI factory + webhook mount helper
  pidfile.py       — single-instance lock
  observability.py — structlog JSON setup

examples/echo_connector/    — 60-line reference adapter
tests/                       — pytest, fakes for IronRAG and adapter
docs/ARCHITECTURE.md         — lifecycle diagram + failure modes
```

## How a request flows

1. `iter_items()` yields `SourceItemRef` (id + kind + external_key +
   change_token + routing_facts).
2. `Router.resolve(ref)` returns `(workspace, library)`.
3. `Orchestrator.push_ref(ref)` reads cursor: if `change_token`
   unchanged AND cursor knows doc_id → `noop_unchanged`, no HTTP call.
4. Else `adapter.fetch(ref)` returns the full `SourceItem`.
5. Orchestrator looks at the cursor's `ironrag_document_id`. If set,
   calls `replace`. If unset, calls `find_document_by_external_key`,
   then `upload` or `replace` per policy.
6. `StateStore.upsert` records the new `change_token` and doc_id.
7. After enumeration finishes, `SyncManager._reap` lists IronRAG docs
   under each kind's external-key prefix and deletes any not seen this
   sweep — subject to `on_missing` policy.

Cursor wins over server find on purpose: some IronRAG deployments do
not expose `external_key` in the list response, and a stale "doc not
found" answer would let us re-upload an existing doc and trigger a PG
unique-violation 500.

## Adding a new connector

1. Clone this repo into your own connector project.
2. Replace `examples/echo_connector` with your real package.
3. Subclass `BaseConnectorSettings` for vendor credentials.
4. Implement `SourceAdapter`. Return one `SourceItemRef` per primary
   item; use `SourceItem.dependents` for attachments/images that ride
   along with the parent.
5. Write a `__main__.py` that does

   ```python
   settings = MySettings()
   adapter = MyAdapter(settings)
   app = build_app(settings, adapter, webhook_handlers=[...])
   uvicorn.run(app, host=settings.host, port=settings.port)
   ```

6. `uv sync --all-extras && uv run pytest && uv run ruff check`.

## Operational notes

- The cursor must be seeded once per IronRAG library that already
  contains connector-owned documents (otherwise the first sweep tries
  to upload every existing item and hits unique-constraint errors). A
  seeder lists existing IronRAG documents under your adapter's
  external-key prefixes, fetches each detail to recover the canonical
  `external_key`, and writes the resulting `(kind, item_id, doc_id)`
  rows into the SQLite cursor.
- `LOG_LEVEL=info` already gives per-document decision lines:
  `sync.item.<action>` events carry `kind`, `item_id`, `external_key`,
  `ironrag_document_id`, `library_id`, `title`, `detail` (the *why*).
- Concurrency, retry, and rate-limit against the vendor API are the
  adapter's responsibility; the framework only bounds parallelism via
  `SYNC_CONCURRENCY`.

## Agent rules

- Treat the framework as the canonical implementation: do not duplicate
  routing / state / push logic in the adapter, extend the framework
  instead.
- When the IronRAG HTTP surface changes, edit `ironrag.py` once; every
  downstream connector inherits the fix.
- New per-kind policies belong as enum values in `policy.py` + a
  matching YAML reader in `routing.py`; do not paper over them with
  ad-hoc adapter flags.
- Persistent cursor state is durable. Do not delete the SQLite file as
  a "fix" without understanding why a row diverged.
- No vendor hostnames, workspace/library UUIDs, or organization-
  specific corpora belong in this repo. Examples and tests must use
  synthetic identifiers only; real deployment values live in operator
  `.env.local` files outside the tree.
