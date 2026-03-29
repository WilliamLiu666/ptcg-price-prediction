from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path


def build_raw_day_dir(source: str, dataset: str) -> Path:
    """
    Build raw data directory path: Data/raw/<source>/<dataset>/YYYY/MM/DD
    """
    now = datetime.now(timezone.utc)
    return Path("Data") / "raw" / source / dataset / f"{now.year:04d}" / f"{now.month:02d}" / f"{now.day:02d}"


def build_timestamped_name(prefix: str, ext: str) -> str:
    """
    Build UTC timestamped filename.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{ts}.{ext}"

def build_staging_partition_dir(source: str, dataset: str, dt: date | datetime) -> Path:
    d = dt.date() if isinstance(dt, datetime) else dt
    return Path("Data") / "staging" / source / dataset / f"extract_date={d:%Y-%m-%d}"