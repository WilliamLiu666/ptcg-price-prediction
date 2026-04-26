from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app.extract.limitless_extractor import LimitlessExtractor
from app.load.limitless_loader import LimitlessLoader
from app.transform.limitless_transformer import LimitlessTransformer
from app.load.limitless_staging_loader import LimitlessStagingLoader
from app.utils.extract_policy import resolve_extract_mode


class LimitlessService:
    """
    Service layer for Limitless.

    Responsibilities:
    - Orchestrate extract -> transform -> load (staging)
    - Reuse local HTML cache for the same extract date when available
    - Keep the business flow simple and readable
    """

    def __init__(
        self,
        extractor: LimitlessExtractor,
        transformer: LimitlessTransformer,
        loader: LimitlessStagingLoader,
        db_loader: LimitlessLoader | None = None,
        raw_html_base_dir: str | Path = "Data/raw/limitless/cards_html",
    ) -> None:
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.db_loader = db_loader
        self.raw_html_base_dir = Path(raw_html_base_dir)

    @staticmethod
    def _normalize_extract_date(extract_date: str | None) -> str:
        """
        Normalize extract_date to YYYY-MM-DD.
        """
        if extract_date is None:
            return datetime.now(timezone.utc).date().isoformat()
        return datetime.fromisoformat(extract_date).date().isoformat()

    @staticmethod
    def _build_html_filename(
        lang: str,
        set_code: str,
        card_code: str,
        filename: str | None = None,
    ) -> str:
        """
        Build html filename.
        """
        if filename:
            return filename if filename.endswith(".html") else f"{filename}.html"
        return f"{lang}_{set_code}_{card_code}.html"

    def _build_html_path(
        self,
        lang: str,
        set_code: str,
        card_code: str,
        extract_date: str,
        filename: str | None = None,
        save_to: str | None = None,
    ) -> Path:
        """
        Resolve local html path.

        Example:
            Data/raw/limitless/cards_html/2026/03/29/en_BLK_2.html
        """
        if save_to:
            return Path(save_to)

        dt = datetime.fromisoformat(extract_date).date()
        html_filename = self._build_html_filename(
            lang=lang,
            set_code=set_code,
            card_code=card_code,
            filename=filename,
        )

        return (
            self.raw_html_base_dir
            / f"{dt.year:04d}"
            / f"{dt.month:02d}"
            / f"{dt.day:02d}"
            / html_filename
        )

    @staticmethod
    def _read_local_html_if_exists(path: Path) -> str | None:
        """
        Read local html if it exists.
        """
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8")

    def run_one(
        self,
        lang: str,
        set_code: str,
        card_code: str,
        filename: str | None = None,
        save_to: str | None = None,
        extract_date: str | None = None,
        overwrite_card_index: bool = False,
    ) -> dict[str, str | float | None]:
        """
        Run the full ETL flow for a single Limitless card page.

        Flow:
        1. Check whether local HTML for the same date already exists
        2. If yes, reuse local HTML
        3. If no, fetch HTML and save locally
        4. Transform HTML into structured record
        5. Write to staging parquet

        Args:
            lang:
                Card language code.
            set_code:
                Series / set code.
            card_code:
                Card code within the series.
            filename:
                Optional local html filename.
            save_to:
                Optional explicit local html path.
            extract_date:
                Business extract date in YYYY-MM-DD.
            overwrite_card_index:
                Whether to overwrite existing card_index parquet.
                Default = False.

        Returns:
            The transformed record.
        """
        normalized_extract_date, update_current = resolve_extract_mode(extract_date)

        html_path = self._build_html_path(
            lang=lang,
            set_code=set_code,
            card_code=card_code,
            extract_date=normalized_extract_date,
            filename=filename,
            save_to=save_to,
        )

        html = self._read_local_html_if_exists(html_path)

        context: dict[str, str | None] = {
            "lang": lang,
            "set_code": set_code,
            "card_code": card_code,
            "source": "limitless",
            "html_path": str(html_path),
        }

        if html is None:
            if not update_current:
                raise FileNotFoundError(
                    "historical replay requires cached Limitless raw HTML: "
                    f"{html_path}"
                )
            html, fetch_context = self.extractor.fetch_html(
                lang=lang,
                set_code=set_code,
                card_code=card_code,
                filename=html_path.stem,
                save_to=str(html_path),
            )
            if fetch_context and isinstance(fetch_context, dict):
                context.update(fetch_context)  # type: ignore
            print(f"[extract] fetched from web: {html_path}")
        else:
            print(f"[extract] reused local html: {html_path}")

        record = self.transformer.transform_card(html, context)

        self.loader.write_limitless_record(
            record,
            extract_date=normalized_extract_date,
            overwrite_card_index=overwrite_card_index,
        )

        if self.db_loader is not None:
            self.db_loader.save_card_index(record)
            self.db_loader.save_card_price(
                record,
                observed_date=normalized_extract_date,
                update_current=update_current,
            )

        return record


if __name__ == "__main__":
    extractor = LimitlessExtractor()
    transformer = LimitlessTransformer()
    loader = LimitlessStagingLoader()

    service = LimitlessService(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        raw_html_base_dir="Data/raw/limitless/cards_html",
    )

    record = service.run_one(
        lang="en",
        set_code="BLK",
        card_code="3",
        extract_date="2026-03-29",
        overwrite_card_index=False,
    )

    print("ETL finished.")
    print(record)
