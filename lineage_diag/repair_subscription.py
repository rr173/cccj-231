"""Release-change subscription: enablement, paged net-change reads.

A workspace may opt into a *subscription* at its current release version.
From then on every successful repair publish also records, inside the same
transaction, the signed net-copy difference of complete release business
rows (see :class:`lineage_diag.repair_store.ReleaseDeltaTracker`), and the
retention window advances with the publish.  Consumers that hold a full
baseline at some version can then pull only the net changes between their
synced version and a fixed target version, page by page, resuming after
restarts with an opaque continuation token.

Read protocol
-------------
* First page: the consumer passes ``from_version`` (the version it has
  fully synced).  The target is pinned to the committed version at that
  moment; later commits never move an in-progress read to a new target.
* Each page carries the fixed ``from_version``/``to_version``, at most
  ``page_size`` changes ordered by the canonical row key (a stable total
  order), ``has_more`` and, while more pages exist, a ``cursor``.
* Re-issuing the same cursor returns the same page (deltas inside the
  window are immutable), and pages partition the ordered change set, so
  every change is delivered exactly once.
* An empty interval still reports ``to_version`` so the consumer knows how
  far it may advance its synced version.

Failure handling
----------------
* A rejected or rolled-back repair publish changes nothing the
  subscription exposes: the version, the recorded deltas and the retention
  boundary all stay put, so consumers simply retry later.
* An illegal start (before enablement, ahead of the committed version) or
  a foreign/contradictory cursor raises :class:`SubscriptionError`
  immediately.
* If the start version has slid out of the retention window -- checked on
  every page, and re-checked after the page query so a concurrent publish
  cannot slip a truncated page past the check -- the read fails with
  ``kind="expired"`` and the consumer must re-fetch a full baseline
  (``repair read``) at the current version and resubscribe from it.  No
  partial result is ever delivered in that case.
"""

from __future__ import annotations

import base64
import binascii
import json
from typing import Any, Dict, Optional

from .model import SubscriptionError
from .repair_store import RepairStore

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 10000
_CURSOR_VERSION = 1


def encode_cursor(sub_id: str, from_version: int, to_version: int,
                  after_key: str) -> str:
    payload = {
        "v": _CURSOR_VERSION,
        "sub": sub_id,
        "from": from_version,
        "to": to_version,
        "after": after_key,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                     separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(token: str) -> Dict[str, Any]:
    """Parse and structurally validate a continuation token."""
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise SubscriptionError(
            "continuation token is not a valid subscription cursor",
            kind="cursor_invalid",
        ) from exc
    if not isinstance(payload, dict) or payload.get("v") != _CURSOR_VERSION:
        raise SubscriptionError(
            "continuation token has an unsupported cursor shape",
            kind="cursor_invalid",
        )
    sub = payload.get("sub")
    from_v = payload.get("from")
    to_v = payload.get("to")
    after = payload.get("after")
    if not isinstance(sub, str) or not sub or \
            not _is_version(from_v) or not _is_version(to_v) or \
            not (after is None or isinstance(after, str)):
        raise SubscriptionError(
            "continuation token is malformed",
            kind="cursor_invalid",
        )
    return {"sub": sub, "from": from_v, "to": to_v, "after": after}


def _is_version(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) \
        and value >= 1


def _check_page_size(page_size: Any) -> int:
    if not isinstance(page_size, int) or isinstance(page_size, bool) or \
            not 1 <= page_size <= MAX_PAGE_SIZE:
        raise SubscriptionError(
            f"page_size must be an integer between 1 and {MAX_PAGE_SIZE}, "
            f"got {page_size!r}",
            kind="invalid_page_size",
        )
    return page_size


def read_subscription_page(store: RepairStore,
                           from_version: Optional[int] = None,
                           cursor: Optional[str] = None,
                           page_size: int = DEFAULT_PAGE_SIZE
                           ) -> Dict[str, Any]:
    """Read one page of net release-row changes from an enabled workspace.

    Exactly one of ``from_version`` (first page) or ``cursor`` (later
    pages) selects the position; passing both is allowed only when the
    from-version agrees with the cursor's pinned start.
    """
    cfg = store.subscription_config()
    if cfg is None:
        raise SubscriptionError(
            "this workspace has no release-change subscription; enable it "
            "with 'repair subscribe enable' first",
            kind="not_enabled",
        )
    page_size = _check_page_size(page_size)
    current = int(store.get_meta("version"))

    if cursor is not None:
        if not isinstance(cursor, str):
            raise SubscriptionError(
                "continuation token must be a string",
                kind="cursor_invalid",
            )
        info = decode_cursor(cursor)
        if info["sub"] != cfg.sub_id:
            raise SubscriptionError(
                "continuation token belongs to a different subscription "
                "(workspace or enablement does not match)",
                kind="cursor_invalid",
            )
        if from_version is not None and from_version != info["from"]:
            raise SubscriptionError(
                f"continuation token is pinned to from_version "
                f"{info['from']}, not {from_version}; a token cannot be "
                "reused with a different start",
                kind="cursor_invalid",
            )
        from_v, to_v, after = info["from"], info["to"], info["after"]
        if to_v > current:
            raise SubscriptionError(
                f"continuation token targets version {to_v} which is ahead "
                f"of the committed version {current}",
                kind="cursor_invalid",
            )
    else:
        if from_version is None:
            raise SubscriptionError(
                "first page requires from_version (the version the "
                "consumer has fully synced); later pages require the "
                "cursor returned by the previous page",
                kind="invalid_start",
            )
        if not _is_version(from_version):
            raise SubscriptionError(
                f"from_version must be a positive integer, got "
                f"{from_version!r}",
                kind="invalid_start",
            )
        from_v, to_v, after = from_version, current, None

    if from_v > current:
        raise SubscriptionError(
            f"from_version {from_v} is ahead of the committed version "
            f"{current}; the consumer cannot have synced a version the "
            "workspace has not published",
            kind="invalid_start",
        )
    if from_v < cfg.enabled_version:
        raise SubscriptionError(
            f"from_version {from_v} predates the subscription, enabled at "
            f"version {cfg.enabled_version}; no change history exists "
            "before that version -- fetch a full baseline at the current "
            "version and subscribe from it",
            kind="invalid_start",
        )
    _check_retention(from_v, current, cfg)

    rows = list(store.iter_subscription_page(from_v, to_v, after,
                                             page_size + 1))
    # A publish committing between the window check and the page query
    # could have collected deltas this page still needs.  Re-validating
    # against the newest committed version guarantees a truncated page is
    # never mistaken for a complete one.
    newest = int(store.get_meta("version"))
    _check_retention(from_v, newest, cfg)

    has_more = len(rows) > page_size
    deliver = rows[:page_size]
    page: Dict[str, Any] = {
        "subscription": cfg.sub_id,
        "from_version": from_v,
        "to_version": to_v,
        "page_size": page_size,
        "changes": [
            {"row": json.loads(row_key), "delta": delta}
            for row_key, delta in deliver
        ],
        "has_more": has_more,
    }
    if has_more:
        page["cursor"] = encode_cursor(cfg.sub_id, from_v, to_v,
                                       deliver[-1][0])
    return page


def _check_retention(from_version: int, current: int, cfg) -> None:
    earliest = max(cfg.enabled_version, current - cfg.retention)
    if from_version < earliest:
        raise SubscriptionError(
            f"from_version {from_version} has slid out of the retention "
            f"window: the earliest consumable start is now {earliest} "
            f"(current version {current}, retention {cfg.retention}). "
            f"Re-fetch a full baseline at version {current} and "
            "resubscribe from it; no partial page is produced",
            kind="expired",
        )
