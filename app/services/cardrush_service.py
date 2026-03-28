from app.extract.cardrush_extractor import CardrushExtractor
from app.transform.cardrush_transformer import CardrushTransformer
from app.load.cardrush_loader import CardrushLoader


class CardrushService:
    def __init__(
        self,
        extractor: CardrushExtractor,
        transformer: CardrushTransformer,
        loader: CardrushLoader,
    ):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def fetch_parse_save(
        self,
        url: str,
        filename: str | None = None,
        save_to: str | None = None,
    ) -> int:
        html, product_group = self.extractor.fetch_html(
            url=url,
            filename=filename,
            save_to=save_to,
        )
        items = self.transformer.parse_products(html)
        return self.loader.save_products(
            product_group=product_group,
            items=items,
            parse_price_func=self.transformer.parse_price,
        )