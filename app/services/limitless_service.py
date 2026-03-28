from __future__ import annotations

from app.extract.limitless_extractor import LimitlessExtractor
from app.transform.limitless_transformer import LimitlessTransformer
from app.load.limitless_loader import LimitlessLoader


class LimitlessService:
    """
    Service layer for Limitless.

    Responsibilities:
    - Orchestrate extract -> transform -> load
    - Keep the business flow simple and readable
    """

    def __init__(
        self,
        extractor: LimitlessExtractor,
        transformer: LimitlessTransformer,
        loader: LimitlessLoader,
    ) -> None:
        """
        Initialize the service with ETL dependencies.
        """
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run_one(
        self,
        lang: str,
        set_code: str,
        card_code: str,
        filename: str | None = None,
        save_to: str | None = None,
    ) -> dict[str, str | float | None]:
        """
        Run the full ETL flow for a single Limitless card page.

        Flow:
        1. Fetch raw HTML
        2. Transform HTML into structured record
        3. Save card index
        4. Save card price

        Args:
            lang:
                Card language.
            set_code:
                Set code.
            card_code:
                Card code.
            filename:
                Optional HTML output filename.
            save_to:
                Optional explicit HTML save path.

        Returns:
            The transformed record.
        """
        html, context = self.extractor.fetch_html(
            lang=lang,
            set_code=set_code,
            card_code=card_code,
            filename=filename,
            save_to=save_to,
        )

        record = self.transformer.transform_card(html, context)

        self.loader.ensure_cards_index_table()
        self.loader.save_card_index(record)
        self.loader.save_card_price(record)

        return record


if __name__ == "__main__":
    extractor = LimitlessExtractor(html_dir="Limitless")
    transformer = LimitlessTransformer()
    loader = LimitlessLoader(db_path="ptcg.sqlite")

    service = LimitlessService(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
    )

    record = service.run_one(
        lang="en",
        set_code="BLK",
        card_code="2",
        filename="en_BLK_2",
    )

    print("ETL finished.")
    print(record)