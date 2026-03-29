from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import requests

from app.data_paths import build_raw_day_dir, build_timestamped_name


class HareruyaExtractor:
    """
    Extract layer for Hareruya.

    Responsibilities:
    - Fetch raw HTML from Hareruya collection pages
    - Fetch raw Shopify JSON from collection products endpoint
    - Optionally save raw responses to local files
    - Extract lightweight crawl context from URL, such as collection_id

    This class does NOT handle:
    - final business transformation rules
    - database writing
    """

    def __init__(self, html_dir: str | Path | None = None, timeout: int = 30) -> None:
        self.html_dir = Path(html_dir) if html_dir else build_raw_day_dir("hareruya", "collections")
        self.html_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
            }
        )

    def fetch_html(
        self,
        url: str,
        filename: str | None = None,
        save_to: str | None = None,
    ) -> tuple[str, dict[str, str | None]]:
        """
        Fetch raw HTML from a Hareruya collection URL.
        """
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()

        html = response.text
        context = self._build_context(url, final_url=response.url)

        if save_to:
            self.save_html(html, save_to)
        elif filename:
            path = self.html_dir / f"{filename}.html"
            self.save_html(html, str(path))
        else:
            path = self._build_output_path(collection_url=url, ext="html")
            self.save_html(html, str(path))

        return html, context

    def fetch_products_json(
        self,
        collection_url: str,
        filename: str | None = None,
        save_to: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, str | None]]:
        """
        Fetch raw JSON from Shopify collection products endpoint.
        """
        json_url = collection_url.rstrip("/") + "/products.json?limit=250"
        response = self.session.get(json_url, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        context = self._build_context(collection_url, final_url=response.url)

        if save_to:
            self.save_json(payload, save_to)
        elif filename:
            path = self.html_dir / f"{filename}.json"
            self.save_json(payload, str(path))
        else:
            path = self._build_output_path(collection_url=collection_url, ext="json")
            self.save_json(payload, str(path))

        return payload, context

    def extract_products_from_payload(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Build a simple normalized product list from raw Shopify payload.
        """
        raw_products = payload.get("products")
        if not isinstance(raw_products, list):
            return []

        products: list[dict[str, Any]] = []
        for product in raw_products:
            if not isinstance(product, dict):
                continue

            name = str(product.get("title", "")).strip()
            handle = product.get("handle")
            product_url = (
                f"https://www.hareruya2.com/products/{handle}" if isinstance(handle, str) else None
            )
            variants = product.get("variants")
            price = None
            if isinstance(variants, list) and variants:
                first = variants[0]
                if isinstance(first, dict):
                    raw_price = first.get("price")
                    try:
                        price = float(raw_price) if raw_price is not None else None
                    except (TypeError, ValueError):
                        price = None

            if name:
                products.append(
                    {
                        "name": name,
                        "price": price,
                        "currency": "JPY",
                        "product_url": product_url,
                    }
                )

        return products

    def fetch_response(self, url: str) -> dict[str, Any]:
        """
        Convenience method that fetches both HTML and JSON and returns parsed products.
        """
        html, html_context = self.fetch_html(url=url)
        payload, json_context = self.fetch_products_json(collection_url=url)
        products = self.extract_products_from_payload(payload)

        return {
            "html_length": len(html),
            "collection_id": html_context.get("collection_id"),
            "html_final_url": html_context.get("final_url"),
            "json_final_url": json_context.get("final_url"),
            "product_count": len(products),
            "products": products,
        }

    def save_html(self, html: str, path: str) -> None:
        """
        Save raw HTML to a local file.
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

    def save_json(self, payload: dict[str, Any], path: str) -> None:
        """
        Save raw JSON to a local file.
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _build_context(url: str, final_url: str) -> dict[str, str | None]:
        """
        Build lightweight crawl context from collection URL.
        """
        match = re.search(r"/collections/([^/?#]+)", url)
        collection_id = match.group(1) if match else None
        return {
            "collection_id": collection_id,
            "source_url": url,
            "final_url": final_url,
        }

    def _build_output_path(self, collection_url: str, ext: str) -> Path:
        collection_id = collection_url.rstrip("/").split("/")[-1] or "unknown"
        filename = build_timestamped_name(prefix=f"collection_{collection_id}", ext=ext)
        return self.html_dir / filename


def main() -> None:
    url = "https://www.hareruya2.com/collections/706"
    extractor = HareruyaExtractor(html_dir=None)

    html, context = extractor.fetch_html(url=url, filename="collection_706_page")
    payload, _ = extractor.fetch_products_json(collection_url=url, filename="collection_706_products")
    products = extractor.extract_products_from_payload(payload)

    print(f"Fetched HTML length: {len(html)}")
    print(context)
    print(f"Parsed {len(products)} products from raw JSON.")
    print(f"Sample: {products[:5]}")


if __name__ == "__main__":
    main()
