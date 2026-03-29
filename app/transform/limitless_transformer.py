from __future__ import annotations

import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from bs4.element import Comment


class LimitlessTransformer:
    """
    Transform layer for Limitless.

    Responsibilities:
    - Parse raw HTML
    - Extract structured fields from the page
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

    def extract_hrefs(self, html: str, prefix: str = "/cards/") -> list[dict[str, str]]:
        """
        Extract internal card hrefs from the page.

        Args:
            html:
                Raw HTML string.
            prefix:
                Only keep hrefs starting with this prefix.

        Returns:
            A list of dictionaries containing:
            - lang
            - set_code
            - card_code
        """
        soup = self._soup(html)
        out: list[dict[str, str]] = []

        for a in soup.find_all("a", href=True):
            href = str(a.get("href", "")).strip() # type: ignore
            if not href.startswith(prefix):
                continue

            parsed = urlparse(href)
            parts = parsed.path.strip("/").split("/")

            # Expected format: /cards/{lang}/{set_code}/{card_code}
            if len(parts) != 4 or parts[0] != "cards":
                continue

            _, lang, set_code, card_code = parts
            out.append({
                "lang": lang,
                "set_code": set_code,
                "card_code": card_code,
            })

        return out

    def extract_ids(self, html: str) -> dict[str, str | None]:
        """
        Extract CARD ID and DATA ID from HTML comments.

        Args:
            html:
                Raw HTML string.

        Returns:
            Dictionary with:
            - card_id
            - data_id
        """
        soup = self._soup(html)

        card_id = None
        data_id = None

        for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
            text = str(c).strip()

            if text.startswith("CARD ID"):
                card_id = text.replace("CARD ID", "").strip()
            elif text.startswith("DATA ID"):
                data_id = text.replace("DATA ID", "").strip()

        return {
            "card_id": card_id,
            "data_id": data_id,
        }

    def extract_rarity(self, html: str) -> str | None:
        """
        Extract rarity from the current print details section.

        Example source text:
            "#63 · Uncommon"

        Returns:
            Rarity string such as 'Uncommon', or None if not found.
        """
        soup = self._soup(html)
        span = soup.select_one(".card-prints-current .prints-current-details span:not(.text-lg)")

        if span is None:
            return None

        text = span.get_text(strip=True)
        if "·" in text:
            return text.split("·", 1)[-1].strip()

        return None

    def extract_price(self, html: str) -> dict[str, float | None]:
        """
        Extract USD and EUR prices from the card page.

        Returns:
            Dictionary with:
            - usd_price
            - eur_price
        """
        soup = self._soup(html)

        usd_tag = soup.select_one("span.card-price.usd")
        eur_tag = soup.select_one("span.card-price.eur")

        usd_price = None
        eur_price = None

        if usd_tag:
            text = usd_tag.get_text(strip=True)
            m = re.search(r"\d+\.?\d*", text)
            if m:
                usd_price = float(m.group())

        if eur_tag:
            text = eur_tag.get_text(strip=True)
            m = re.search(r"\d+\.?\d*", text)
            if m:
                eur_price = float(m.group())

        return {
            "usd_price": usd_price,
            "eur_price": eur_price,
        }

    def extract_name(self, html: str) -> str | None:
        """
        Extract card name from the card title section.
        """
        soup = self._soup(html)
        name_tag = soup.select_one(".card-text-name a")
        return name_tag.get_text(strip=True) if name_tag else None

    def transform_card(self, html: str, context: dict[str, str | None]) -> dict[str, str | float | None]:
        """
        Transform one raw Limitless page into a normalized card record.

        Args:
            html:
                Raw HTML string.
            context:
                Crawl context returned from extractor.

        Returns:
            A normalized dictionary containing:
            - card_id
            - data_id
            - lang
            - set_code
            - card_code
            - card_name
            - rarity
            - usd_price
            - eur_price
            - card_path
        """
        ids = self.extract_ids(html)
        rarity = self.extract_rarity(html)
        prices = self.extract_price(html)
        name = self.extract_name(html)

        return {
            "card_id": ids["card_id"],
            "data_id": ids["data_id"],
            "lang": context.get("lang"),
            "set_code": context.get("set_code"),
            "card_code": context.get("card_code"),
            "card_name": name,
            "rarity": rarity,
            "usd_price": prices["usd_price"],
            "eur_price": prices["eur_price"],
            "card_path": context.get("card_path"),
        }


if __name__ == "__main__":
    from app.data_paths import build_raw_day_dir

    html_path = build_raw_day_dir("limitless", "cards_html") / "en_BLK_2.html"
    html = html_path.read_text(encoding="utf-8")

    transformer = LimitlessTransformer()
    context: dict[str, str | None] = {
        "lang": "en",
        "set_code": "BLK",
        "card_code": "2",
        "card_path": "/cards/en/BLK/2",
    }

    record = transformer.transform_card(html, context)
    print(record)