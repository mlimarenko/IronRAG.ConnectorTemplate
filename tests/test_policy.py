from __future__ import annotations

from ironrag_connector.policy import (
    DeleteAction,
    DuplicateContentAction,
    PolicyOverride,
    PushPolicy,
    UpdateAction,
    UpsertAction,
)


def test_default_policy_is_full_lifecycle() -> None:
    policy = PushPolicy()
    assert policy.on_new is UpsertAction.CREATE
    assert policy.on_changed is UpdateAction.REPLACE
    assert policy.on_missing is DeleteAction.DELETE
    assert policy.on_duplicate_content is DuplicateContentAction.SKIP


def test_merge_only_overrides_set_fields() -> None:
    base = PushPolicy()
    override = PolicyOverride(on_missing=DeleteAction.IGNORE)
    merged = base.merged_with(override)
    assert merged.on_missing is DeleteAction.IGNORE
    assert merged.on_new is UpsertAction.CREATE
    assert merged.on_changed is UpdateAction.REPLACE
