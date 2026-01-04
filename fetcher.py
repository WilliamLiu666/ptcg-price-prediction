import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class SimpleFetcher:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

    def fetch_html(self, url: str) -> str:
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.text

    def save_html(self, html: str, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

    def parse_products(self, html: str, base_url: str = "https://www.cardrush-pokemon.jp") -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        items = []

        # 你的商品最外层就是这个
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
