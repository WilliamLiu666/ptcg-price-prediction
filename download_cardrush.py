# download_cardrush.py
from fetcher import SimpleFetcher

fetcher = SimpleFetcher()

html = fetcher.fetch_html(
    "https://www.cardrush-pokemon.jp/product-group/267"
)

fetcher.save_html(html, "cardrush/cardrush_267.html")

print("done")

