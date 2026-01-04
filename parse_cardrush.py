from fetcher import SimpleFetcher

fetcher = SimpleFetcher()
items = fetcher.parse_products_from_html_file("cardrush/cardrush_267.html")

print("解析到商品数量：", len(items))
for item in items[:5]:
    print(item)

