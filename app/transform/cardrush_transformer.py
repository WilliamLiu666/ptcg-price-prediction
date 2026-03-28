import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag


class CardrushTransformer:
    @staticmethod
    def parse_products(html: str, base_url: str = "https://www.cardrush-pokemon.jp") -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        items: list[dict] = []

        product_divs = soup.find_all("div", class_="item_data")
        for div in product_divs:
            if not isinstance(div, Tag):
                continue

            product_id = div.get("data-product-id")

            a = div.find("a", class_="item_data_link")
            href = a.get("href") if isinstance(a, Tag) else None
            if isinstance(href, list):
                href = href[0] if href else None
            product_url = urljoin(base_url, href) if href else None

            name_span = div.select_one("p.item_name span.goods_name")
            name_full = name_span.get_text(strip=True) if name_span else None

            name = None
            condition = None
            model_number = None
            set_size = None

            if name_full:
                m_cond = re.search(r"〔(.*?)〕", name_full)
                condition = m_cond.group(1) if m_cond else None

                name = re.sub(r"〔.*?〕|\(.*?\)|【.*?】|\{.*?\}", "", name_full).strip()

                m = re.search(r"\{(\d+)\s*/\s*(\d+)\}", name_full)
                if m:
                    model_number = m.group(1)
                    set_size = m.group(2)

            model_span = div.select_one("p.item_name span.model_number_value")
            raw_model = model_span.get_text(strip=True) if model_span else None

            model_code = None
            if raw_model:
                m2 = re.search(r"\](.+)$", raw_model)
                model_code = m2.group(1).strip() if m2 else raw_model.strip()

            price_span = div.select_one("p.selling_price span.figure")
            price_text = price_span.get_text(strip=True) if price_span else None

            items.append({
                "product_id": product_id,
                "name_full": name_full,
                "name": name,
                "condition": condition,
                "model_number": model_number,
                "set_size": set_size,
                "model_code": model_code,
                "price": price_text,
                "product_url": product_url,
            })

        return items

    @staticmethod
    def parse_price(price_str: str | None) -> float | None:
        if not price_str:
            return None
        m = re.search(r"([\d,]+)", price_str)
        return float(m.group(1).replace(",", "")) if m else None