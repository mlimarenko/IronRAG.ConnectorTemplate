"""Contract every connector implements.

A connector wires a single :class:`SourceAdapter` instance into the
framework. The adapter is the only piece of code that knows the vendor
API; everything else (routing, policy, state, sync loop, HTTP server) is
shared infrastructure.

The adapter yields two shapes:

* :class:`SourceItemRef` — lightweight summary used during the diff stage.
  Just enough to decide whether the item needs to be (re)fetched. Must
  carry a ``change_token`` (typically the source's ``updated_at`` or an
  ETag) so the framework can short-circuit unchanged items without
  downloading their payload.
* :class:`SourceItem` — fully materialized payload (bytes + mime + title
  + idempotency hint) returned by :meth:`SourceAdapter.fetch`. The
  framework hands this to the IronRAG client.

The ``kind`` field on both shapes is the framework's per-policy bucket
key. A single adapter can emit several kinds (pages, attachments,
images) and the operator can apply different push/delete policies to
each via ``routing.yaml``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class SourceItemRef:
    """Lightweight handle used during a sweep's diff stage."""

    item_id: str
    """Stable, source-side identity (page id, file path, document key)."""

    kind: str
    """Bucket for per-kind policy. Free-form (e.g. ``page``, ``attachment``)."""

    external_key: str
    """IronRAG identity surface, e.g. ``my-source:page:42``.

    Must be deterministic from ``(kind, item_id)`` for the lifetime of the
    item, so renames and slug changes still resolve to the same IronRAG
    document.
    """

    change_token: str | None = None
    """Opaque token that advances iff the item's content may have changed.

    Typical sources: ``updated_at`` ISO timestamp, ETag, revision counter,
    content hash. If ``None`` the framework cannot diff and will always
    re-fetch.
    """

    routing_facts: dict[str, Any] = field(default_factory=dict)
    """Adapter-emitted facts the Router can match on (shelf, book, tag…)."""

    raw: dict[str, Any] = field(default_factory=dict)
    """Original source payload, opaque to the framework. Useful for logs."""


@dataclass(frozen=True)
class SourceItem:
    """Full payload ready to be pushed into IronRAG."""

    ref: SourceItemRef
    payload: bytes
    mime_type: str
    file_name: str
    title: str | None = None
    idempotency_key: str | None = None
    """Override the per-call idempotency key.

    Default is computed from ``(connector_name, kind, item_id, change_token)``
    when omitted. Content-addressed items (images, blobs) typically pass an
    explicit key derived from the content hash so identical bytes uploaded
    from different parents collapse into one IronRAG document.
    """

    dependents: tuple[SourceItem, ...] = ()
    """Items derived from this one (a page's attachments and inline images).

    Returned inline so the orchestrator can push them in the same
    transaction as their parent. Dependents are themselves valid
    SourceItems and obey their own ``kind``'s policy.
    """


@runtime_checkable
class SourceAdapter(Protocol):
    """Vendor-specific code lives here. Everything else is framework."""

    name: str
    """Short slug, e.g. ``my-source``. Used as the external-key prefix."""

    kinds: tuple[str, ...]
    """Every ``kind`` value the adapter can emit (primary + dependent)."""

    primary_kinds: tuple[str, ...]
    """Kinds the adapter enumerates from ``iter_items()`` directly.

    The framework's orphan reaper only walks these kinds: a primary
    kind that was not seen during the sweep is safe to delete because
    we know iter_items enumerated every live item. Dependent kinds
    (attachments, inline images) reached only through a parent's
    fetch are excluded from reaping — their lifecycle follows the
    parent's.
    """

    async def iter_items(self) -> AsyncIterator[SourceItemRef]:
        """Yield every primary item visible in the source.

        Dependents (a page's attachments, a doc's images) should NOT be
        yielded here — they ride along with their parent via
        :attr:`SourceItem.dependents`. The reaper still discovers them on
        the IronRAG side via the kind→prefix scan, so orphans of vanished
        parents are collected on the next sweep.
        """
        if False:  # pragma: no cover — type-only yield
            yield  # type: ignore[unreachable]

    async def fetch(self, ref: SourceItemRef) -> SourceItem | None:
        """Materialize the full payload for ``ref``.

        Returns ``None`` when the source no longer has the item (race with
        a concurrent delete on the vendor side). The framework treats
        ``None`` here as a soft miss for this sweep; the reaper handles the
        actual orphan deletion based on the absence of the ref in the
        sweep set, not based on a fetch result.
        """
        ...

    def external_key(self, kind: str, item_id: str) -> str:
        """Compose the canonical IronRAG external_key for an item.

        Used by the reaper (which only has the external key from IronRAG)
        to round-trip back to a ``(kind, item_id)`` pair via
        :meth:`parse_external_key`.
        """
        ...

    def parse_external_key(self, external_key: str) -> tuple[str, str] | None:
        """Inverse of :meth:`external_key`. Returns ``(kind, item_id)`` or
        ``None`` if the key was not minted by this adapter."""
        ...

    async def close(self) -> None:
        """Release any HTTP clients or file handles the adapter holds."""
        ...
