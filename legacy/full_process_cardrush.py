# download_cardrush.py
from __future__ import annotations

import re
import sqlite3
from typing import List, Tuple
from urllib.parse import urljoin

from cardrushFetcher import CardrushFetcher


# ---------- Helpers ----------

def safe_filename(value: str) -> str:
    """
    Convert a string into a filesystem-safe filename.
    """
    value = value.strip()
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value)


# ---------- Database ----------

def load_series_urls(
    db_path: str,
    source: str = "cardrush"
) -> List[Tuple[str, str]]:
    """
    Load series list URLs for a given source from the series_url_jp table.

    Args:
        db_path: Path to the SQLite database file.
        source: Data source name used to filter series URLs.

    Returns:
        A list of (series_code, list_url) tuples.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = """
    SELECT
        series_code,
        list_url
    FROM series_url_jp
    WHERE LOWER(source) = LOWER(?)
      AND list_url IS NOT NULL
      AND TRIM(list_url) <> ''
    ORDER BY series_code
    """
    cur.execute(sql, (source,))
    rows = cur.fetchall()
    conn.close()

    return [(row["series_code"], row["list_url"]) for row in rows]


# ---------- Crawling ----------
def build_page_url(base_url: str, page_index: int, page_size: int = 100) -> str:
    """
    Build a paginated CardRush listing URL from a base series URL.

    Args:
        base_url: Base series listing URL.
        page_index: Page number to fetch (1-based).
        page_size: Number of items per page.

    Returns:
        A fully constructed paginated URL string.
    """
    path = f"0/photo?num={page_size}&page={page_index}"
    return urljoin(base_url.rstrip("/") + "/", path)

def page_fingerprint(items: list[dict]) -> tuple | None:
    """
    Generate a fingerprint for a product page to detect pagination loops.

    Args:
        items: A list of parsed product dictionaries from a single page.

    Returns:
        A tuple fingerprint derived from the first product on the page,
        or None if the page contains no items.
    """
    if not items:
        return None

    # Use the first product as a stable page signature
    first = items[0]
    return (
        first.get("product_id"),
        first.get("product_url")
    )


def crawl_series(
    fetcher: CardrushFetcher,
    series_code: str,
    base_url: str,
    max_pages: int = 14
) -> None:
    """
    Crawl all paginated product listing pages for a single series and persist results.

    Args:
        fetcher: CardRush fetcher instance used for HTTP requests and persistence.
        series_code: Normalized series identifier (e.g. SV1, SV2a).
        base_url: Base series listing URL.
        max_pages: Maximum number of pages to attempt before stopping.

    Returns:
        None
    """
    series_code_safe = safe_filename(series_code)

    # Fingerprint of page 1, used to detect pagination loops
    first_page_fp = None

    for page_index in range(1, max_pages + 1):
        page_url = build_page_url(base_url, page_index)

        print(f"[CardRush] Fetching {series_code} page {page_index}: {page_url}")

        html = fetcher.fetch_html(
            page_url,
            filename=f"{series_code_safe}_{page_index}"
        )

        items = fetcher.parse_products(html)

        # Stop if the page contains no products
        if not items:
            print(f"[CardRush] Empty page for {series_code}, stop paging.")
            break

        current_fp = page_fingerprint(items)

        # Record fingerprint from the first page
        if page_index == 1:
            first_page_fp = current_fp
        else:
            # Stop when pagination loops back to page 1
            if current_fp == first_page_fp:
                print(
                    f"[CardRush] Pagination overflow detected for {series_code} "
                    f"(page {page_index} == page 1). Stop."
                )
                break

        # Attach series_code for downstream joins and analysis
        for item in items:
            item["series_code"] = series_code

        saved = fetcher.save_products_to_sqlite(items)
        print(f"[CardRush] Saved {saved} items for {series_code} page {page_index}")

# ---------- Entry ----------
def main() -> None:
    """
    Entry point for CardRush crawling.
    """
    db_path = "ptcg.sqlite"

    fetcher = CardrushFetcher(
        db_path=db_path,
        html_dir="cardrush"
    )

    series_urls = load_series_urls(db_path, source="cardrush")

    print(f"[CardRush] Loaded {len(series_urls)} series URLs.")

    for series_code, base_url in series_urls:
        crawl_series(fetcher, series_code, base_url)


if __name__ == "__main__":
    main()
