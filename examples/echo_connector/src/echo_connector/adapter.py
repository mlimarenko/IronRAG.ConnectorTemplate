"""Trivial SourceAdapter against an in-memory dict."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass

from ironrag_connector import SourceAdapter, SourceItem, SourceItemRef


@dataclass
class EchoPage:
    item_id: str
    title: str
    body: str
    updated_at: str
    tag: str | None = None


class EchoAdapter(SourceAdapter):
    name = "echo"
    kinds = ("page",)
    primary_kinds = ("page",)

    def __init__(self, pages: dict[str, EchoPage]) -> None:
        self._pages = pages

    async def iter_items(self) -> AsyncIterator[SourceItemRef]:
        for page in self._pages.values():
            yield SourceItemRef(
                item_id=page.item_id,
                kind="page",
                external_key=self.external_key("page", page.item_id),
                change_token=page.updated_at,
                routing_facts={"tag": page.tag} if page.tag else {},
                raw={"title": page.title},
            )

    async def fetch(self, ref: SourceItemRef) -> SourceItem | None:
        page = self._pages.get(ref.item_id)
        if page is None:
            return None
        return SourceItem(
            ref=ref,
            payload=page.body.encode("utf-8"),
            mime_type="text/markdown",
            file_name=f"{page.item_id}.md",
            title=page.title,
        )

    def external_key(self, kind: str, item_id: str) -> str:
        return f"{self.name}:{kind}:{item_id}"

    def parse_external_key(self, external_key: str) -> tuple[str, str] | None:
        prefix = f"{self.name}:"
        if not external_key.startswith(prefix):
            return None
        rest = external_key[len(prefix) :]
        kind, _, item_id = rest.partition(":")
        if not kind or not item_id:
            return None
        return kind, item_id

    async def close(self) -> None:
        return None
