from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup


class HareruyaTransformer:
    """
    Transform layer for Hareruya.

    Responsibilities:
    - Parse raw HTML / Shopify JSON payload
    - Extract structured card fields from Hareruya collection products
    - Normalize extracted values into Python dictionaries

    This class does NOT handle:
    - HTTP requests
    - file saving
    - database writing
    """

    @staticmethod
    def _soup(html: str) -> BeautifulSoup:
        """
        Parse HTML into BeautifulSoup.
        """
        return BeautifulSoup(html, "html.parser")

    @staticmethod
    def _to_float(value: Any) -> float | None:
        """
        Convert a value to float safely.
        """
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _to_str(value: Any) -> str | None:
        """
        Convert a value to stripped string safely.
        """
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _extract_first_variant(product: dict[str, Any]) -> dict[str, Any] | None:
        """
        Extract first variant object from product.
        """
        variants = product.get("variants")
        if isinstance(variants, list) and variants:
            first = variants[0]
            if isinstance(first, dict):
                return first
        return None

    @staticmethod
    def _extract_first_image_url(product: dict[str, Any]) -> str | None:
        """
        Extract first image URL from product images.
        """
        images = product.get("images")
        if isinstance(images, list):
            for image in images:
                if isinstance(image, dict):
                    src = image.get("src")
                    if isinstance(src, str) and src.strip():
                        return src.strip()
        return None

    def extract_collection_title(self, html: str) -> str | None:
        """
        Extract collection title from collection page HTML.
        """
        soup = self._soup(html)

        selectors = [
            "h1",
            ".collection-hero__title",
            ".section-header__title",
            ".main-page-title",
        ]

        for selector in selectors:
            tag = soup.select_one(selector)
            if tag:
                text = tag.get_text(strip=True)
                if text:
                    return text

        return None

    def extract_products(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Extract raw product objects from Shopify payload.
        """
        products = payload.get("products")
        if isinstance(products, list):
            return [p for p in products if isinstance(p, dict)]
        return []

    def extract_title_parts(self, title: str | None) -> dict[str, str | None]:
        """
        Parse Hareruya product title.

        Example:
            ナゾノクサ(C){草}〈001/080〉[M2]
            メガリザードンXex(SAR){炎}〈110/080〉[M2]

        Extract:
            - card_name_jp
            - rarity
            - card_type_jp
            - card_number
            - total_in_set
            - set_code
        """
        if not title:
            return {
                "card_name_jp": None,
                "rarity": None,
                "card_type_jp": None,
                "card_number": None,
                "total_in_set": None,
                "set_code": None,
            }

        pattern = re.compile(
            r"""
            ^(?P<card_name_jp>.+?)          # 卡名
            \((?P<rarity>[^)]+)\)           # 稀有度
            \{(?P<card_type_jp>[^}]+)\}     # 属性/种类
            〈(?P<card_number>\d+)\/(?P<total_in_set>\d+)〉
            \[(?P<set_code>[^\]]+)\]
            $
            """,
            re.VERBOSE,
        )

        m = pattern.match(title.strip())
        if not m:
            return {
                "card_name_jp": title.strip(),
                "rarity": None,
                "card_type_jp": None,
                "card_number": None,
                "total_in_set": None,
                "set_code": None,
            }

        return {
            "card_name_jp": m.group("card_name_jp").strip(),
            "rarity": m.group("rarity").strip(),
            "card_type_jp": m.group("card_type_jp").strip(),
            "card_number": m.group("card_number").strip(),
            "total_in_set": m.group("total_in_set").strip(),
            "set_code": m.group("set_code").strip(),
        }

    def extract_body_fields(self, body_html: str | None) -> dict[str, str | None]:
        """
        Extract extra metadata from body_html.

        Example body_html snippet:
            図鑑番号：43
            名前：ナゾノクサ
            英語名：Oddish

        Returns:
            - zukan_number
            - body_name_jp
            - card_name_en
        """
        if not body_html:
            return {
                "zukan_number": None,
                "body_name_jp": None,
                "card_name_en": None,
            }

        soup = self._soup(body_html)
        text = soup.get_text("\n", strip=True)

        zukan_number = None
        body_name_jp = None
        card_name_en = None

        m = re.search(r"図鑑番号[:：]\s*([^\n\r]+)", text)
        if m:
            zukan_number = m.group(1).strip()

        m = re.search(r"名前[:：]\s*([^\n\r]+)", text)
        if m:
            body_name_jp = m.group(1).strip()

        m = re.search(r"英語名[:：]\s*([^\n\r]+)", text)
        if m:
            card_name_en = m.group(1).strip()

        return {
            "zukan_number": zukan_number,
            "body_name_jp": body_name_jp,
            "card_name_en": card_name_en,
        }

    def transform_product(
        self,
        product: dict[str, Any],
        context: dict[str, str | None],
    ) -> dict[str, Any]:
        """
        Transform one raw Hareruya product into a normalized card record.
        """
        title = self._to_str(product.get("title"))
        title_parts = self.extract_title_parts(title)
        body_fields = self.extract_body_fields(self._to_str(product.get("body_html")))
        variant = self._extract_first_variant(product)

        handle = self._to_str(product.get("handle"))
        product_url = (
            f"https://www.hareruya2.com/products/{handle}"
            if handle
            else None
        )

        tags = product.get("tags")
        if not isinstance(tags, list):
            tags = []

        return {
            "source": "hareruya",
            "collection_id": context.get("collection_id"),
            "source_url": context.get("source_url"),
            "final_url": context.get("final_url"),

            # 原始字段
            "product_id": self._to_str(product.get("id")),
            "handle": handle,
            "title_raw": title,
            "body_html": self._to_str(product.get("body_html")),
            "vendor": self._to_str(product.get("vendor")),
            "product_type": self._to_str(product.get("product_type")),
            "published_at": product.get("published_at"),
            "created_at": product.get("created_at"),
            "updated_at": product.get("updated_at"),

            # 业务字段：从 title 拆
            "card_name_jp": title_parts["card_name_jp"],
            "rarity": title_parts["rarity"],
            "card_type_jp": title_parts["card_type_jp"],
            "card_number": title_parts["card_number"],
            "total_in_set": title_parts["total_in_set"],
            "set_code": title_parts["set_code"],

            # 业务字段：从 body_html 拆
            "zukan_number": body_fields["zukan_number"],
            "body_name_jp": body_fields["body_name_jp"],
            "card_name_en": body_fields["card_name_en"],

            # 销售字段
            "variant_id": self._to_str(variant.get("id")) if variant else None,
            "variant_title": self._to_str(variant.get("title")) if variant else None,
            "sku": self._to_str(variant.get("sku")) if variant else None,
            "available": bool(variant.get("available")) if variant and variant.get("available") is not None else None,
            "price_jpy": self._to_float(variant.get("price")) if variant else None,
            "compare_at_price_jpy": self._to_float(variant.get("compare_at_price")) if variant else None,
            "currency": "JPY",

            # 其他
            "product_url": product_url,
            "image_url": self._extract_first_image_url(product),
            "tags": [str(tag).strip() for tag in tags if str(tag).strip()],
        }

    def transform_products(
        self,
        payload: dict[str, Any],
        context: dict[str, str | None],
    ) -> list[dict[str, Any]]:
        """
        Transform all raw products in Shopify payload into normalized records.
        """
        products = self.extract_products(payload)
        return [self.transform_product(product, context) for product in products]

    def transform_collection(
        self,
        html: str,
        payload: dict[str, Any],
        context: dict[str, str | None],
    ) -> dict[str, Any]:
        """
        Transform one Hareruya collection into collection-level structured output.
        """
        records = self.transform_products(payload, context)
        collection_title = self.extract_collection_title(html)

        return {
            "source": "hareruya",
            "collection_id": context.get("collection_id"),
            "collection_title": collection_title,
            "source_url": context.get("source_url"),
            "final_url": context.get("final_url"),
            "product_count": len(records),
            "products": records,
        }


if __name__ == "__main__":
    import json
    from pathlib import Path

    html_path = Path("Data/raw/hareruya/collections/2026/03/29/collection_706_page.html")
    json_path = Path("Data/raw/hareruya/collections/2026/03/29/collection_706_products.json")

    html = html_path.read_text(encoding="utf-8")
    payload = json.loads(json_path.read_text(encoding="utf-8"))

    transformer = HareruyaTransformer()
    context: dict[str, str | None] = {
        "collection_id": "706",
        "source_url": "https://www.hareruya2.com/collections/706",
        "final_url": "https://www.hareruya2.com/collections/706",
    }

    result = transformer.transform_collection(html, payload, context)

    print(f"Collection title: {result['collection_title']}")
    print(f"Product count: {result['product_count']}")
    print(result["products"][:3])