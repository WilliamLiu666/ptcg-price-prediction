from __future__ import annotations

import argparse

from app.jobs.ebay_batch_job import run_batch as run_ebay_batch
from app.jobs.hareruya_batch_job import run_batch as run_hareruya_batch
from app.jobs.limitless_batch_job import run_batch as run_limitless_batch


DEFAULT_SOURCES: tuple[str, ...] = ("limitless", "hareruya", "ebay")


def run_batch(
    extract_date: str | None = None,
    overwrite_card_index: bool = False,
    sources: list[str] | tuple[str, ...] | None = None,
) -> None:
    """
    Run one or more source batch jobs from a single app-level entry point.

    Default order is:
    1. Limitless
    2. Hareruya
    3. eBay
    """
    selected_sources = tuple(sources or DEFAULT_SOURCES)

    for source in selected_sources:
        print(f"\n=== Running {source} batch ===")

        if source == "limitless":
            run_limitless_batch(
                extract_date=extract_date,
                overwrite_card_index=overwrite_card_index,
            )
            continue

        if source == "hareruya":
            run_hareruya_batch(
                extract_date=extract_date,
                overwrite_card_index=overwrite_card_index,
            )
            continue

        if source == "ebay":
            run_ebay_batch(
                extract_date=extract_date,
                overwrite_card_index=overwrite_card_index,
            )
            continue

        raise ValueError(f"Unsupported source: {source}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run all source batches from one app-level command."
    )
    parser.add_argument(
        "--extract-date",
        default=None,
        help=(
            "Partition date YYYY-MM-DD. Today UTC runs live fetch + current update; "
            "older dates replay cached raw files only and do not overwrite current."
        ),
    )
    parser.add_argument(
        "--overwrite-card-index",
        action="store_true",
        help="Overwrite existing card_index parquet files",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=DEFAULT_SOURCES,
        default=list(DEFAULT_SOURCES),
        help="Subset of source jobs to run (default: all)",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    run_batch(
        extract_date=args.extract_date,
        overwrite_card_index=args.overwrite_card_index,
        sources=args.sources,
    )


if __name__ == "__main__":
    main()
