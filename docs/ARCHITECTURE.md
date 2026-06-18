# Architecture

## Where the framework sits

```
┌──────────────────┐  vendor REST / webhook  ┌─────────────────────────────┐
│  vendor source   │ ────────────────────────│  your-connector             │
│  (wiki / CMS /   │                         │   ┌──────────────────────┐  │
│   filesystem)    │                         │   │   YourSourceAdapter  │  │
└──────────────────┘                         │   └──────────────────────┘  │
                                             │              │              │
                                             │   ironrag_connector         │
                                             │   ┌──────────────────────┐  │
                                             │   │ routing / state /    │  │
                                             │   │ policy / sync /      │  │
                                             │   │ orchestrator / http  │  │
                                             │   └──────────┬───────────┘  │
                                             └──────────────┼──────────────┘
                                                            │
                                                            │ POST /v1/content/documents/upload
                                                            │ POST /v1/content/documents/{id}/replace
                                                            │ DELETE /v1/content/documents/{id}
                                                            ▼
                                                ┌────────────────────────┐
                                                │        IronRAG         │
                                                └────────────────────────┘
```

The adapter is the only piece of code that knows the vendor API.
Everything else is framework.

## Lifecycle of one item

```
iter_items ──► SourceItemRef (id, kind, external_key, change_token, facts)
                       │
                       ▼
                  Router.resolve
                       │
            ┌──────────┴──────────┐
            │                     │
        matched               unrouted
            │                     │
            ▼                     ▼
    StateStore.get        OrchestrationOutcome(unrouted)
            │
   ┌────────┴────────┐
   │                 │
change_token       change_token
unchanged          advanced (or no row)
   │                 │
   ▼                 ▼
ironrag.find    adapter.fetch(ref)
   │                 │
existing?         SourceItem (bytes, mime, title, dependents)
   │                 │
   ▼                 ▼
noop_unchanged   Orchestrator.push_item
                       │
              ┌────────┼────────┐
              │        │        │
        not present  present  on 409 duplicate
              │        │        │
              ▼        ▼        ▼
       on_new=...   on_changed=...   on_duplicate_content=...
              │        │        │
              ▼        ▼        ▼
       created  replaced  skipped_duplicate_content
                       │
                       ▼
              StateStore.upsert
```

The reaper runs after enumeration completes successfully: for every
primary `kind` the adapter declares, the framework walks every IronRAG
list page under the adapter's external-key prefix and deletes any active
document whose `(kind, item_id)` was not seen this sweep — subject to the
kind's `on_missing` policy.

The comparison also includes the routed library. Cursor rows remember
which IronRAG library owns their document id, and each sweep takes a
pre-push snapshot of those cursor libraries. If a connector's routing
rules move a still-existing source item from one library to another, the
orchestrator creates or updates the document in the newly resolved
library, and the reaper scans the previous cursor library to delete the
old target document. If an item was enumerated but did not establish a
successful route/push target in the current sweep, existing documents are
kept to avoid turning transient fetch or routing failures into
destructive deletes.

Cursor databases created before `ironrag_library_id` existed are
transitioned explicitly. Before enumeration, the framework backfills at most
`CURSOR_LIBRARY_LOOKUP_MAX_ROWS_PER_SWEEP` rows by asking IronRAG for document
detail with a bounded `CURSOR_LIBRARY_LOOKUP_TIMEOUT_SECONDS` timeout. During
per-item processing, a legacy row first checks the currently resolved target
library by external key; when found, the row is upgraded without needing a
document-detail lookup. This ownership upgrade does not advance the stored
`change_token`; only a successful create/replace or an explicit skip policy
marks the current source version as handled. If ownership still cannot be
proven, the sweep stays non-fatal and non-destructive for the unknown historical
library; route-move cleanup for that unresolved row is deferred to a later
sweep. The item path still refuses to upload a possible duplicate while
ownership is unknown, so degraded backfill preserves duplicate-prevention over
eager cleanup.

## Why a Protocol instead of inheritance

`SourceAdapter` is a `typing.Protocol` so adapters can be plain classes
without imports from the framework's class hierarchy. This keeps the
framework dependency surface flat (you only import the data shapes you
return) and makes adapters trivially mockable in tests.

## Why SQLite for state

The diff stage compares each `change_token` against the value stored at
the last successful push. JSON works until two writers touch the file at
once (manual `/sync/run` overlapping a periodic sweep, two workers in
docker compose, …). SQLite gives atomic per-row upsert at zero
operational cost — one file, no daemon, fsync per write — and the
schema is `(kind TEXT, item_id TEXT, change_token TEXT, external_key
TEXT, ironrag_document_id TEXT, last_pushed_at TEXT)` with primary key
`(kind, item_id)`.

## Why per-kind policy

Different items derived from the same source have different update
shapes. Pages: create + replace + delete. Images: create + replace +
*ignore* on missing (an image dropped from one page may still be
referenced by another). Drafts: skip new + skip changed + delete on
missing (only push when published).

Making policy a per-`kind` map keeps the framework's identity model
honest: every push decision is `(kind, item_id) → policy`.

## Failure modes

| Symptom | Framework behaviour |
|---------|---------------------|
| `iter_items` raises mid-stream | Sweep aborts, reaper is **not** run (would falsely delete unseen items). Next sweep retries. |
| `fetch` returns `None` | Outcome `fetch_returned_none`; no IronRAG call, no cursor update. Reaper will still delete the cursor row on the next clean sweep where the ref was missing. |
| IronRAG 5xx | Raised as `IronRagError`; orchestrator surfaces in `sync.errors` and continues with the next item. |
| Legacy cursor library lookup times out | Sweep continues with partial reaper coverage; unresolved historical libraries are not deleted in that sweep, and duplicate uploads remain blocked until ownership is known. |
| IronRAG 409 duplicate | `on_duplicate_content` policy decides: silently dedupe (default) or raise. |
| Vendor returns retry-able status (429 / 5xx) | The adapter's transport layer is responsible for retry-with-backoff. The framework does not retry on the vendor side; see `bookstack/src/bookstack_connector/bookstack.py:97` for a reference implementation. |
| Two processes start with the same `STATE_DB_PATH` | The pidfile lock fails the second one loud. SQLite would otherwise handle the concurrency, but two parallel sweeps would burn vendor quota. |

## Webhook handlers

The framework does not assume a vendor webhook format. Adapters that
*do* receive webhooks register one or more `WebhookHandler`s; the
framework mounts each at `/webhook/{name}`, does bearer-token auth,
parses JSON, and hands the dict to the handler. The handler is expected
to translate the payload into one or more `SourceItemRef`s and call
`Orchestrator.push_ref` on each — same code path as the sync loop.

This keeps webhooks and periodic sync coherent: a `page_update` webhook
runs through identical routing + policy + state machinery as the same
page surfacing on the next sweep.

## What the framework deliberately does NOT do

- It does not assume any vendor's authentication, retry, or pagination
  shape. That lives in the adapter's transport layer.
- It does not own a vendor-specific data model. Books, shelves,
  databases, spaces, notebooks — all of those are facts the adapter
  emits via `routing_facts` and the routing config can match on.
- It does not retry IronRAG calls. IronRAG's own idempotency keys make
  the same request safe to re-send by the operator; in-process retry
  belongs to a future enhancement, not the canonical path.
- It does not hold an in-process queue. The sync loop is the queue:
  each periodic pass is a fresh, idempotent sweep.
