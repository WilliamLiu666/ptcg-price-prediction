import requests
from bs4 import BeautifulSoup
from pathlib import Path

URL = "https://limitlesstcg.com/cards/jp/SVP/1?translate=en"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def main():
    resp = requests.get(URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    html = resp.text

    # 1️⃣ 保存 HTML，方便人工分析
    out = Path("limitless_card_1.html")
    out.write_text(html, encoding="utf-8")
    print(f"[OK] HTML saved to {out.resolve()}")

    soup = BeautifulSoup(html, "html.parser")

    # 2️⃣ 页面标题
    print("\n=== <title> ===")
    print(soup.title.string if soup.title else "No title")

    # 3️⃣ Meta 信息（常有图片 / card name）
    print("\n=== <meta> tags ===")
    for m in soup.find_all("meta"):
        if m.get("property") or m.get("name"):
            print({
                "name": m.get("name"),
                "property": m.get("property"),
                "content": m.get("content")
            })

    # 4️⃣ Script 标签（重点：找 JSON / __NEXT_DATA__）
    print("\n=== <script> tags (truncated) ===")
    for i, s in enumerate(soup.find_all("script")):
        text = (s.string or "").strip()
        if not text:
            continue

        print(f"\n--- Script #{i} ---")
        print(text[:500])  # 只打印前 500 字，防止刷屏

if __name__ == "__main__":
    main()
