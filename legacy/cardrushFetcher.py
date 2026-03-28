import re
import sqlite3
from datetime import datetime, timezone
from urllib.parse import urljoin
from pathlib import Path

import requests
from bs4 import BeautifulSoup, Tag


class CardrushFetcher:
    def __init__(self, db_path: str = "ptcg.sqlite", html_dir: str | None = None):
        """
        Initializes the fetcher with database and optional HTML storage settings.

        Args:
            db_path (str): Path to the SQLite database file used to store scraped data.
                        Defaults to "ptcg.sqlite".
            html_dir (str | None): Optional directory path for saving fetched HTML files.
                                - If None, HTML files will NOT be saved to disk.
                                - If provided, all fetched HTML content can be
                                    persisted locally for debugging or auditing.
        """

        # Store the database file path for later database operations
        self.db_path = db_path

        # If html_dir is provided, convert it to a Path object.
        # If html_dir is None, disable HTML persistence.
        self.html_dir = Path(html_dir) if html_dir else None

        # Default HTTP headers for all outgoing requests.
        # Setting a User-Agent helps avoid being blocked by some websites.
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }


    # ---------- Network & File I/O ----------
    def fetch_html(self, url: str, filename: str | None = None, save_to: str | None = None) -> str:
        """
        Fetch HTML content from a given URL.

        This method is designed to be reusable for batch crawling:
        a single CardrushFetcher instance can be used to fetch multiple URLs
        sequentially. Each call to this method updates the instance's
        `self.product_group` attribute based on the URL being fetched.

        Parameters
        ----------
        url : str
            Target page URL to fetch (e.g. a product-group / series URL).

        filename : str | None, optional
            Base filename (without extension) used when saving the HTML.
            Only used when `save_to` is not provided and `self.html_dir` is set.
            If omitted, a UTC timestamp-based filename will be generated
            automatically (when saving is enabled).

        save_to : str | None, optional
            Explicit file path to save the HTML.
            If provided, this path has the highest priority and will be used
            regardless of `filename` or `self.html_dir`.

        Returns
        -------
        str
            Raw HTML content of the fetched page.

        Saving Behavior
        ---------------
        HTML saving follows this priority order:
        1. If `save_to` is provided → save to this exact path.
        2. Else if `self.html_dir` is set and `filename` is provided
            → save as `<html_dir>/<filename>.html`.
        3. Else if `self.html_dir` is set
            → save using an auto-generated UTC timestamp filename.
        4. Else
            → HTML is not saved to disk.

        Notes
        -----
        - This method performs network I/O and optional file system I/O.
        - The same instance can safely fetch multiple different URLs.
        - Saving HTML is optional and mainly useful for debugging or offline re-parsing.
        """

        # Attempt to extract the product-group ID from the URL.
        # Example: /product-group/12345 → product_group = "12345"
        match = re.search(r"/product-group/(\d+)", url)

        # Store the extracted product group ID on the instance.
        # This introduces a side effect: calling this method updates
        # `self.product_group`, which may be used by downstream logic.
        self.product_group = match.group(1) if match else None

        # Send an HTTP GET request to fetch the page content.
        # A timeout is applied to avoid hanging indefinitely.
        response = requests.get(url, headers=self.headers, timeout=30)

        # Raise an exception for non-2xx HTTP responses.
        response.raise_for_status()

        # Extract raw HTML text from the response.
        html = response.text

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


    def save_html(self, html: str, path: str) -> None:
        """
        Save raw HTML content to a local file.

        Parameters
        ----------
        html : str
            Raw HTML content to be saved.

        path : str
            Target file path (including filename) where the HTML
            content will be written.

        Returns
        -------
        None
        """

        # Ensure the parent directory of the target file exists.
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        # Write the HTML content to the file using UTF-8 encoding.
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

    # ---------- Parsing ----------
    def parse_products(self, html: str, base_url: str = "https://www.cardrush-pokemon.jp") -> list[dict]:
        """
        Parse product information from a CardRush HTML page.

        This method extracts structured product data from the raw HTML,
        including product identifiers, names, conditions, card numbers,
        model codes, prices, and product URLs.

        Parameters
        ----------
        html : str
            Raw HTML content of a product list page.

        base_url : str, optional
            Base URL used to construct absolute product URLs when
            relative links are encountered.

        Returns
        -------
        list[dict]
            A list of dictionaries, where each dictionary represents
            one product with parsed fields such as name, price, and IDs.
        """

        # Parse the HTML document using BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Container for all parsed product records
        items: list[dict] = []

        # Locate all product blocks on the page
        product_divs = soup.find_all("div", class_="item_data")
        for div in product_divs:
            # Defensive check: ensure the element is a valid Tag
            if not isinstance(div, Tag):
                continue

            # Extract the internal product ID from data attributes
            product_id = div.get("data-product-id")

            # ---------- product URL ----------
            # Locate the anchor tag linking to the product detail page
            a = div.find("a", class_="item_data_link")
            href = a.get("href") if isinstance(a, Tag) else None

            # Some parsers may return attribute values as lists
            if isinstance(href, list):
                href = href[0] if href else None

            # Build an absolute URL using the base URL
            product_url = urljoin(base_url, href) if href else None

            # ---------- name parsing ----------
            # Extract the full raw product name text
            name_span = div.select_one("p.item_name span.goods_name")
            name_full = name_span.get_text(strip=True) if name_span else None
            # Example:
            # "〔状態A-〕かがやくゲッコウガ(K仕様)【-】{003/019}"

            name = None
            condition = None

            if name_full:
                # Extract card condition enclosed in brackets, e.g. 状態A-
                m_cond = re.search(r"〔(.*?)〕", name_full)
                condition = m_cond.group(1) if m_cond else None

                # Remove decorations such as condition, variants, symbols,
                # and card number info to obtain the pure card name
                name = re.sub(
                    r"〔.*?〕|\(.*?\)|【.*?】|\{.*?\}",
                    "",
                    name_full
                ).strip()

            # ---------- card number & set size ----------
            model_number = None   # e.g. "003"
            set_size = None       # e.g. "019"

            if name_full:
                # Extract card number and set size from "{003/019}"
                m = re.search(r"\{(\d+)\s*/\s*(\d+)\}", name_full)
                if m:
                    model_number = m.group(1)
                    set_size = m.group(2)

            # ---------- model code (e.g. SVJP) ----------
            model_span = div.select_one("p.item_name span.model_number_value")
            raw_model = model_span.get_text(strip=True) if model_span else None

            model_code = None
            if raw_model:
                # Remove leading decorations and keep the trailing model code
                m2 = re.search(r"\](.+)$", raw_model)
                model_code = m2.group(1).strip() if m2 else raw_model.strip()

            # ---------- price ----------
            # Extract displayed selling price
            price_span = div.select_one("p.selling_price span.figure")
            price_text = price_span.get_text(strip=True) if price_span else None

            # Assemble the parsed product record
            items.append({
                "product_id": product_id,
                "name_full": name_full,        # original full name text
                "name": name,                  # cleaned card name
                "condition": condition,        # card condition (e.g. 状態A-)
                "model_number": model_number,  # card number within the set
                "set_size": set_size,          # total cards in the set
                "model_code": model_code,      # set / series code (e.g. SVJP)
                "price": price_text,
                "product_url": product_url,
            })

        # Return all parsed product entries
        return items

    def parse_products_from_html_file(self, html_path: str) -> list[dict]:
        """
        Parse product information from a local HTML file using the existing HTML parsing logic.

        Parameters
        ----------
        html_path : str
            Path to the local HTML file containing a product list page.

        Returns
        -------
        list[dict]
            A list of parsed product dictionaries extracted from the HTML file.
        """

        # Read the HTML content from the local file
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()

        # Delegate parsing to the shared HTML parser
        return self.parse_products(html)


    # ---------- SQLite ----------
    @staticmethod
    def _parse_price(price_str: str | None) -> float | None:
        """
        Convert a price string with currency formatting into a numeric value.

        Parameters
        ----------
        price_str : str | None
            Price string extracted from HTML (e.g. "79,800円").

        Returns
        -------
        float | None
            Parsed numeric price (e.g. 79800.0), or None if input is invalid.
        """

        # Return None if the input is empty or missing
        if not price_str:
            return None

        # Extract the numeric part of the price (digits and commas)
        m = re.search(r"([\d,]+)", price_str)

        # Remove thousand separators and convert to float
        return float(m.group(1).replace(",", "")) if m else None


    def save_products_to_sqlite(self, items: list[dict]) -> int:
        """
        Persist parsed CardRush product data into SQLite tables for products and price snapshots.

        Parameters
        ----------
        items : list[dict]
            Parsed product records extracted from HTML pages.

        Returns
        -------
        int
            Number of product records successfully written to the database.
        """

        # Generate a single observation timestamp for this batch
        now = datetime.now(timezone.utc)
        observed_at = now.isoformat()

        # Product group is expected to be set during HTML fetching
        product_group = getattr(self, "product_group", None)
        if not product_group:
            raise RuntimeError("product_group is not set on fetcher")

        written = 0

        # Open SQLite connection using WAL mode for better write performance
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            cur = conn.cursor()

            # Process each parsed product record
            for it in items:
                product_id = it.get("product_id")
                url = it.get("product_url") or it.get("url")

                name = it.get("name")
                name_full = it.get("name_full")
                condition = it.get("condition")

                model_number = it.get("model_number")   # e.g. 003
                set_size = it.get("set_size")           # e.g. 019
                model_code = it.get("model_code")       # e.g. SVJP

                # Convert price text into numeric JPY value
                price_yen = self._parse_price(it.get("price"))

                # ---------- required field validation ----------
                if not product_id or not url or not name or not name_full:
                    continue
                if not model_number:
                    continue
                if price_yen is None:
                    continue

                # ---------- 1) upsert product metadata ----------
                cur.execute("""
                INSERT INTO products_cardrush (
                    product_id, product_group, model_number, set_size,
                    name, name_full, condition, model_code,
                    price_yen, url,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    product_group  = excluded.product_group,
                    model_number = excluded.model_number,
                    set_size     = excluded.set_size,
                    name         = excluded.name,
                    name_full    = excluded.name_full,
                    condition    = excluded.condition,
                    model_code   = excluded.model_code,
                    price_yen    = excluded.price_yen,
                    url          = excluded.url,
                    updated_at   = excluded.updated_at
                """, (
                    product_id, product_group, model_number, set_size,
                    name, name_full, condition, model_code,
                    price_yen, url,
                    observed_at, observed_at
                ))

                # ---------- 2) append price snapshot ----------
                cur.execute("""
                INSERT INTO prices_cardrush (
                    product_id, observed_at, price_yen
                )
                VALUES (?, ?, ?)
                """, (product_id, observed_at, price_yen))

                written += 1

            # Commit all changes in a single transaction
            conn.commit()

        return written


if __name__ == "__main__":
    fetcher = CardrushFetcher(
        db_path="ptcg.sqlite",
        html_dir="cardrush"  # ✅ 改动1：默认存到这个目录（可改）
    )

    url = "https://www.cardrush-pokemon.jp/product-group/268"

    # ✅ 改动2：分三步走
    html = fetcher.fetch_html(url, filename = 'group268')                 # 拉取（会自动存 raw_html/page_*.html）
    items = fetcher.parse_products(html)           # 解析
    n = fetcher.save_products_to_sqlite(items)     # 入库
    print(f"✅ 写入完成：{n} 条")
