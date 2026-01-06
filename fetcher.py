import re
import sqlite3
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


class SimpleFetcher:
    def __init__(self, db_path: str = "ptcg.sqlite"):
        self.db_path = db_path
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

    # ---------- 网络与文件 ----------
    def fetch_html(self, url: str) -> str:
        response = requests.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.text

    def save_html(self, html: str, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

    # ---------- 解析 ----------
    def parse_products(self, html: str, base_url: str = "https://www.cardrush-pokemon.jp") -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        items = []

        product_divs = soup.find_all("div", class_="item_data")

        for div in product_divs:
            product_id = div.get("data-product-id")

            a = div.find("a", class_="item_data_link")
            href = a.get("href") if a else None
            product_url = urljoin(base_url, href) if href else None

            name_span = div.select_one("p.item_name span.goods_name")
            name = name_span.get_text(strip=True) if name_span else None

            model_span = div.select_one("p.item_name span.model_number_value")
            model_number = model_span.get_text(strip=True) if model_span else None

            price_span = div.select_one("p.selling_price span.figure")
            price_text = price_span.get_text(strip=True) if price_span else None  # 例如 "79,800円"

            stock_p = div.select_one("p.stock")
            stock_text = stock_p.get_text(strip=True) if stock_p else None  # 例如 "在庫数 2枚"

            img = div.select_one("div.global_photo img")
            img_url = img.get("src") if img else None
            img_alt = img.get("alt") if img else None

            items.append({
                "product_id": product_id,
                "name": name,
                "model_number": model_number,
                "price": price_text,
                "stock": stock_text,
                "product_url": product_url,
                "image_url": img_url,
                "image_alt": img_alt,
            })

        return items

    def parse_products_from_html_file(self, html_path: str) -> list[dict]:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        return self.parse_products(html)

    @staticmethod
    def _parse_price(price_str: str | None) -> float | None:
        """'79,800円' -> 79800"""
        if not price_str:
            return None
        m = re.search(r"([\d,]+)", price_str)
        return float(m.group(1).replace(",", "")) if m else None

    def save_products_to_sqlite(self, items: list[dict], currency: str = "JPY") -> int:
        """
        将 items 写入 sqlite：
        - products：upsert
        - price_history：按天一条（同一天重复抓取会覆盖）
        返回：成功写入（处理）的 product 数量
        """
        now = datetime.now(timezone.utc)
        captured_at = now.isoformat()
        captured_date = now.date().isoformat()

        written = 0
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            cur = conn.cursor()

            for it in items:
                product_id = it.get("product_id")
                name = it.get("name")
                url = it.get("product_url")

                if not product_id or not name or not url:
                    continue

                # 1) products
                cur.execute("""
                INSERT INTO products (product_id, url, name, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                  url = excluded.url,
                  name = excluded.name,
                  updated_at = excluded.updated_at
                """, (product_id, url, name, captured_at, captured_at))

                # 2) price_history（每天一条）
                cur.execute("""
                INSERT INTO price_history (
                  product_id, captured_date, captured_at, price, currency, stock_status
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id, captured_date) DO UPDATE SET
                  captured_at  = excluded.captured_at,
                  price        = excluded.price,
                  currency     = excluded.currency,
                  stock_status = excluded.stock_status
                """, (
                    product_id,
                    captured_date,
                    captured_at,
                    self._parse_price(it.get("price")),
                    currency,
                    it.get("stock")
                ))

                written += 1

            conn.commit()

        return written

    # ---------- 一条龙 ----------
    def fetch_parse_and_save(self, url: str) -> int:
        """
        1) 拉取页面
        2) 解析商品
        3) 写入 sqlite
        返回：写入条数（处理的商品数）
        """
        html = self.fetch_html(url)
        items = self.parse_products(html)
        return self.save_products_to_sqlite(items)


if __name__ == "__main__":
    fetcher = SimpleFetcher(db_path="ptcg.sqlite")
    # 示例：把 url 替换成你的列表页链接
    url = "https://www.cardrush-pokemon.jp/product-group/267"
    n = fetcher.fetch_parse_and_save(url)
    print(f"✅ 写入完成：{n} 条")
