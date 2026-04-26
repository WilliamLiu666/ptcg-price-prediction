from __future__ import annotations

from datetime import datetime, timezone


def normalize_extract_date(extract_date: str | None) -> str:
    """
    Normalize an extract date to YYYY-MM-DD using UTC when omitted.
    """
    if extract_date is None:
        return datetime.now(timezone.utc).date().isoformat()
    return datetime.fromisoformat(extract_date).date().isoformat()


def today_utc_date() -> str:
    """
    Return today's date in UTC as YYYY-MM-DD.
    """
    return datetime.now(timezone.utc).date().isoformat()


def resolve_extract_mode(extract_date: str | None) -> tuple[str, bool]:
    """
    Resolve extract date and whether this is a live/current-day run.

    Returns:
        (normalized_extract_date, update_current)
    """
    normalized_extract_date = normalize_extract_date(extract_date)
    today = today_utc_date()

    if normalized_extract_date > today:
        raise ValueError(
            f"extract_date={normalized_extract_date} is in the future; "
            f"today_utc={today}"
        )

    return normalized_extract_date, normalized_extract_date == today


def resolve_observed_timestamps(
    observed_date: str | None = None,
    observed_at: str | None = None,
) -> tuple[str, str]:
    """
    Resolve a normalized observation timestamp/date pair.

    If only ``observed_date`` is provided, midnight UTC is used for
    ``observed_at`` so historical replays remain deterministic.
    """
    if observed_date:
        normalized_date = datetime.fromisoformat(observed_date).date().isoformat()
        if observed_at:
            normalized_at = datetime.fromisoformat(observed_at).isoformat()
        else:
            normalized_at = f"{normalized_date}T00:00:00+00:00"
        return normalized_at, normalized_date

    if observed_at:
        dt = datetime.fromisoformat(observed_at)
        return dt.isoformat(), dt.date().isoformat()

    now = datetime.now(timezone.utc)
    return now.isoformat(), now.date().isoformat()
