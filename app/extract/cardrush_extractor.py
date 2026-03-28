import re
from datetime import datetime, timezone
from pathlib import Path
from urllib import response

import requests


class CardrushExtractor:
    """
    Extract layer for Cardrush.

    Responsibilities:
    - Fetch raw HTML from Cardrush pages
    - Optionally save fetched HTML to local files
    - Extract lightweight crawl context from URL, such as product_group

    This class does NOT handle:
    - HTML parsing
    - field cleaning / transformation
    - database writing
    """

    def __init__(self, html_dir: str | None = None):
        self.html_dir = Path(html_dir) if html_dir else None

        # 比只放 User-Agent 更完整一些
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "ja,en-GB;q=0.9,en;q=0.8",
            "Referer": "https://www.cardrush-pokemon.jp/",
            "Connection": "keep-alive",
        }

        # 用 Session 比单次 requests.get 更像正常访问
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def fetch_html(
        self,
        url: str,
        filename: str | None = None,
        save_to: str | None = None,
    ) -> tuple[str, str | None]:
        """
        Fetch raw HTML from a Cardrush URL.
        """
        match = re.search(r"/product-group/(\d+)", url)
        product_group = match.group(1) if match else None

        try:
            response = self.session.get(url, timeout=30)
            print("status:", response.status_code)
            print("final_url:", response.url)
            print("content_type:", response.headers.get("Content-Type"))
            print("preview:", response.text[:300])
            response.raise_for_status()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            raise RuntimeError(f"HTTP request failed with status={status} for url={url}") from e

        html = response.text

        if save_to:
            self.save_html(html, save_to)
        elif self.html_dir and filename:
            self.html_dir.mkdir(parents=True, exist_ok=True)
            path = self.html_dir / f"{filename}.html"
            self.save_html(html, str(path))
        elif self.html_dir:
            self.html_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            path = self.html_dir / f"page_{ts}.html"
            self.save_html(html, str(path))

        return html, product_group

    def save_html(self, html: str, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)


if __name__ == "__main__":
    extractor = CardrushExtractor(html_dir="cardrush")
    url = "https://www.cardrush-pokemon.jp/product-group/268"

    html, product_group = extractor.fetch_html(url, filename="group268")
    print(f"Fetched HTML length: {len(html)}")
    print(f"Product group: {product_group}")