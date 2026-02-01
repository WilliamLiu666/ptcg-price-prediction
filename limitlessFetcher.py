from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Comment


class LimitlessFetcher:
    def __init__(
        self,
        html_dir: str | Path,
        db_path: str | Path | None = None,
        timeout: int = 30,
        headers: dict | None = None
    ) -> None:
        self.html_dir = Path(html_dir)
        self.html_dir.mkdir(parents=True, exist_ok=True)

        self.db_path = Path(db_path) if db_path is not None else None
        self.timeout = timeout

        self.headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # ✅ 你在 fetch_html 里用到了，但之前没初始化
        self.last_card_path: str | None = None

    # 1) 获取html
    def fetch_html(self, lang: str, set_code: str, card_code: str, filename: str | None = None, save_to: str | None = None) -> str:
        """
        Fetch HTML content from a given URL.

        Saving priority:
        1) save_to
        2) html_dir + filename
        3) html_dir + timestamp
        4) no save
        """
        self.lang = lang
        self.set_code = set_code
        self.card_code = card_code
        url = f"https://limitlesstcg.com/cards/{lang}/{set_code}/{card_code}"
        if lang == "jp":
            url += "?translate=en"
        m = re.search(r"(\/cards\/[^?#]+)", url)
        self.last_card_path = m.group(1) if m else None

        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        html = resp.text

        # ---------- HTML saving logic ----------
        # Priority:
        # 1) Explicit save_to path
        # 2) html_dir + filename
        # 3) html_dir + auto-generated timestamp
        # 4) Do not save
        if save_to:
            # Highest priority: save to the explicitly provided path
            self.save_html(html, save_to)

        elif self.html_dir and filename:
            # Ensure the HTML directory exists
            self.html_dir.mkdir(parents=True, exist_ok=True)

            # Construct the output path using the provided filename
            path = self.html_dir / f"{filename}.html"
            self.save_html(html, str(path))

        elif self.html_dir:
            # Ensure the HTML directory exists
            self.html_dir.mkdir(parents=True, exist_ok=True)

            # Generate a UTC timestamp-based filename for uniqueness
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            path = self.html_dir / f"page_{ts}.html"
            self.save_html(html, str(path))

        # Return the raw HTML content for immediate parsing or processing
        return html

    # 2) 储存html
    def save_html(self, html: str, path: str) -> None:
        """
        Save raw HTML content to a local file.
        """
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

    @staticmethod
    def _soup(html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    # 3) 获取href list
    def extract_hrefs(self, html: str, prefix: str = "/cards/") -> list[dict[str, str]]:
        soup = self._soup(html)
        out: list[dict[str, str]] = []
        for a in soup.find_all("a", href=True):
            href = str(a.get("href", "")).strip()
            if href.startswith(prefix):
                parsed = urlparse(href)
                parts = parsed.path.strip("/").split("/")
                if len(parts) != 4 or parts[0] != "cards":
                    pass
                else:
                    _, lang, set_code, card_code = parts
                    out.append({
                        "lang": lang,
                        "set_code": set_code,
                        "card_code": card_code,
                        })
        return out

    # 4) 获取rarity
    def extract_rarity(self, html: str) -> dict[str, str | None]:
        soup = self._soup(html)
        span = soup.select_one(".card-prints-current .prints-current-details span:not(.text-lg)")

        if span is None:
            rarity = None
        else:
            text = span.get_text(strip=True)  # "#63 · Uncommon"
            if "·" in text:
                rarity = text.split("·", 1)[-1].strip()
            else:
                rarity = None
        
        card_id = None
        data_id = None

        for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
            text = str(c).strip()

            if text.startswith("CARD ID"):
                card_id = text.replace("CARD ID", "").strip()
            elif text.startswith("DATA ID"):
                data_id = text.replace("DATA ID", "").strip()

        self.rarity = rarity
        self.card_id = card_id
        self.data_id = data_id

        return {
            "card_id": card_id,
            "data_id": data_id,
            "rarity": rarity
        }


    def ensure_cards_index_table(self) -> None:
        """
        Ensure the cards_index table exists in the SQLite database.

        Target schema:
        - PK: card_id (from Limitless HTML comment)
        - data_id column
        - UNIQUE(lang, set_code, card_code) for convenient mapping
        """
        if self.db_path is None:
            raise RuntimeError("db_path is not set. Please pass db_path when creating LimitlessFetcher.")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")

            conn.executescript("""
            CREATE TABLE IF NOT EXISTS cards_index (
              card_id   INTEGER PRIMARY KEY,   -- CARD ID (limitlesstcg)
              data_id   INTEGER,               -- DATA ID (limitlesstcg)

              lang      TEXT NOT NULL,
              set_code  TEXT NOT NULL,
              card_code TEXT NOT NULL,
              rarity    TEXT,

              UNIQUE(lang, set_code, card_code)
            );

            CREATE INDEX IF NOT EXISTS idx_cards_index_lang_set
              ON cards_index(lang, set_code);

            CREATE INDEX IF NOT EXISTS idx_cards_index_rarity
              ON cards_index(rarity);

            -- 可选：如果你经常按 data_id 查再打开
            -- CREATE INDEX IF NOT EXISTS idx_cards_index_data_id
            --   ON cards_index(data_id);
            """)
            conn.commit()

    def save_card_index(self) -> None:
        """
        Upsert one card record into cards_index using card_id as PK.

        - Conflict target: card_id
        - Update policy:
          - data_id: update only when excluded.data_id is not None
          - rarity:  update only when excluded.rarity is not None
          - lang/set_code/card_code: always keep in sync with current fetch context
        """
        if self.db_path is None:
            raise RuntimeError("db_path is not set. Please pass db_path when creating LimitlessFetcher.")

        # 从当前 fetch context 取值（你现在的写法）
        lang = self.lang
        set_code = self.set_code
        card_code = self.card_code
        card_id = self.card_id
        data_id = self.data_id
        rarity = self.rarity

        # Ensure table exists

        self.ensure_cards_index_table()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")

            conn.execute(
                """
                INSERT INTO cards_index (card_id, data_id, lang, set_code, card_code, rarity)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(card_id)
                DO UPDATE SET
                  -- keep mapping keys in sync
                  lang = excluded.lang,
                  set_code = excluded.set_code,
                  card_code = excluded.card_code,

                  -- only update if new values exist
                  data_id = CASE
                    WHEN excluded.data_id IS NOT NULL THEN excluded.data_id
                    ELSE cards_index.data_id
                  END,
                  rarity = CASE
                    WHEN excluded.rarity IS NOT NULL THEN excluded.rarity
                    ELSE cards_index.rarity
                  END
                """,
                (card_id, data_id, lang, set_code, card_code, rarity),
            )
            conn.commit()

if __name__ == "__main__":
    fetcher = LimitlessFetcher(html_dir="Limitless", db_path="ptcg.sqlite")
    #html = fetcher.fetch_html(lang="jp", set_code="SV11B", card_code="2")
    html = fetcher.fetch_html(lang="en", set_code="BLK", card_code="2")
    hrefs = fetcher.extract_hrefs(html)
    print(hrefs)
    rarity = fetcher.extract_rarity(html)
    fetcher.save_card_index()