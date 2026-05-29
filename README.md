<h1 align="center">IronRAG Connector Template</h1>
<p align="center"><b>Python framework for building data-source connectors that push content into <a href="https://github.com/mlimarenko/IronRAG">IronRAG</a>.</b></p>

<p align="center">
  <a href="https://github.com/mlimarenko/IronRAG.ConnectorTemplate/releases"><img src="https://img.shields.io/github/v/release/mlimarenko/IronRAG.ConnectorTemplate?style=flat-square&label=release" alt="Release"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="License"></a>
  <img src="https://img.shields.io/badge/python-3.12%2B-blue?style=flat-square" alt="Python">
</p>

---

You implement one [`SourceAdapter`](src/ironrag_connector/source.py) Protocol — the framework handles the rest:

- IronRAG HTTP client (`upload` / `replace` / `delete` / `find_by_external_key`).
- YAML routing: arbitrary adapter-emitted facts → `(workspace, library)`.
- Per-`kind` push policies (`on_new` / `on_changed` / `on_missing` / `on_duplicate_content`).
- Persistent SQLite cursor so restarts ship only the diff.
- Periodic sync loop, orphan reaper, in-sweep dedup.
- FastAPI server: `/health`, `/sync/run`, optional `/webhook/{name}` mounts.
- Pidfile lock, structured per-item logging, run modes (`poll` / `webhook` / `both`).

## Reference connector

[`mlimarenko/IronRAG.BookStack`](https://github.com/mlimarenko/IronRAG.BookStack) — production BookStack adapter built on this template, with multimodal page+image+attachment kinds.

## Quick start

```bash
git clone git@github.com:mlimarenko/IronRAG.ConnectorTemplate.git my-connector
cd my-connector
cp .env.example .env.local            # set IRONRAG_BASE_URL, IRONRAG_API_TOKEN, ADMIN_BEARER_TOKEN
cp routing.yaml.example routing.yaml  # set workspace/library UUIDs

uv sync --all-extras
uv run pytest
```

Then replace [`examples/echo_connector/`](examples/echo_connector/) with your adapter package, subclass `BaseConnectorSettings` for vendor credentials, and hand the adapter to `build_app`.

## The contract

Adapters use `external_key` as the technical sync identity: it must stay stable
so the connector can find, replace, and reap the right IronRAG document. A
`SourceItem` may also set `document_hint` to a canonical URL or any other
user-facing label that IronRAG can surface to MCP agents in citations; it is
separate from `external_key` and does not participate in sync identity.

## Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — item lifecycle, failure modes, design rationale.
- [CLAUDE.md](CLAUDE.md) — guide for future agents/devs working on the framework.

## Deploy

A connector built on this template ships its own `Dockerfile` (staging the
framework as `framework/`) and a `docker-compose.yml`. See
[`docker-compose.example.yml`](docker-compose.example.yml) for the canonical
shape — env-file secrets, read-only config mounts, and a persistent state
volume — and the [reference connectors](#related) for working instances.

## Related

- [IronRAG](https://github.com/mlimarenko/IronRAG) — the RAG backend connectors feed.
- Connectors built on this template: [Confluence](https://github.com/mlimarenko/IronRAG.Confluence) · [BookStack](https://github.com/mlimarenko/IronRAG.BookStack) · [Git Repositories](https://github.com/mlimarenko/IronRAG.GitRepos)

## License

MIT — see [LICENSE](LICENSE).
