from __future__ import annotations

import re
from pathlib import Path

import requests

from app.data_paths import build_raw_day_dir, build_timestamped_name


class LimitlessExtractor:
    """
    Extract layer for Limitless.

    Responsibilities:
    - Build Limitless card URL
    - Fetch raw HTML from Limitless
    - Optionally save fetched HTML locally
    - Return lightweight crawl context (lang / set_code / card_code / card_path)

    This class does NOT handle:
    - HTML parsing
    - data cleaning
    - database writing
    """

    def __init__(
        self,
        html_dir: str | Path | None = None,
        timeout: int = 30,
        headers: dict | None = None,
    ) -> None:
        """
        Initialize the extractor.

        Args:
            html_dir:
                Directory used to save fetched HTML files.
            timeout:
                Request timeout in seconds.
            headers:
                Optional custom HTTP headers.
        """
        self.html_dir = Path(html_dir) if html_dir else build_raw_day_dir("limitless", "cards_html")
        self.html_dir.mkdir(parents=True, exist_ok=True)

        self.timeout = timeout
        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def build_card_url(self, lang: str, set_code: str, card_code: str) -> str:
        """
        Build the target Limitless card URL.

        For Japanese pages, append '?translate=en'.

        Args:
            lang: Card language, e.g. 'en' or 'jp'
            set_code: Set code, e.g. 'BLK'
            card_code: Card number/code, e.g. '2'

        Returns:
            Full Limitless card URL.
        """
        url = f"https://limitlesstcg.com/cards/{lang}/{set_code}/{card_code}"
        if lang == "jp":
            url += "?translate=en"
        return url

    def fetch_html(
        self,
        lang: str,
        set_code: str,
        card_code: str,
        filename: str | None = None,
        save_to: str | None = None,
    ) -> tuple[str, dict[str, str | None]]:
        """
        Fetch raw HTML from a Limitless card page.

        Saving priority:
        1. save_to
        2. html_dir + filename
        3. html_dir + auto-generated timestamp filename
        4. do not save

        Args:
            lang:
                Card language.
            set_code:
                Set code.
            card_code:
                Card code.
            filename:
                Optional file name (without extension) when saving to html_dir.
            save_to:
                Optional explicit output file path.

        Returns:
            tuple:
                - html: raw HTML string
                - context: crawl context including lang / set_code / card_code / card_path
        """
        url = self.build_card_url(lang=lang, set_code=set_code, card_code=card_code)

        # Extract a lightweight card path such as:
        # /cards/en/BLK/2
        m = re.search(r"(\/cards\/[^?#]+)", url)
        card_path = m.group(1) if m else None

        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        html = response.text

        if save_to:
            self.save_html(html, save_to)
        elif filename:
            path = self.html_dir / f"{filename}.html"
            self.save_html(html, str(path))
        else:
            path = self.html_dir / build_timestamped_name(prefix="page", ext="html")
            self.save_html(html, str(path))

        context = {
            "lang": lang,
            "set_code": set_code,
            "card_code": card_code,
            "card_path": card_path,
        }

        return html, context

    def save_html(self, html: str, path: str) -> None:
        """
        Save raw HTML to a local file.

        Args:
            html:
                Raw HTML string.
            path:
                Target file path.
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        print(f"Saving HTML to {path}...")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)


if __name__ == "__main__":
    extractor = LimitlessExtractor()

    html, context = extractor.fetch_html(
        lang="en",
        set_code="BLK",
        card_code="2",
        filename="en_BLK_2",
    )

    print(f"Fetched HTML length: {len(html)}")
    print(context)