from __future__ import annotations

import unittest
from unittest.mock import call, patch

from app import main


class MainBatchJobTests(unittest.TestCase):
    @patch("app.main.run_ebay_batch")
    @patch("app.main.run_hareruya_batch")
    @patch("app.main.run_limitless_batch")
    def test_run_batch_runs_all_sources_in_default_order(
        self,
        mock_limitless,
        mock_hareruya,
        mock_ebay,
    ) -> None:
        with patch("builtins.print") as mock_print:
            main.run_batch(
                extract_date="2026-04-26",
                overwrite_card_index=True,
            )

        self.assertEqual(
            mock_print.call_args_list,
            [
                call("\n=== Running limitless batch ==="),
                call("\n=== Running hareruya batch ==="),
                call("\n=== Running ebay batch ==="),
            ],
        )
        mock_limitless.assert_called_once_with(
            extract_date="2026-04-26",
            overwrite_card_index=True,
        )
        mock_hareruya.assert_called_once_with(
            extract_date="2026-04-26",
            overwrite_card_index=True,
        )
        mock_ebay.assert_called_once_with(
            extract_date="2026-04-26",
            overwrite_card_index=True,
        )

    @patch("app.main.run_ebay_batch")
    @patch("app.main.run_hareruya_batch")
    @patch("app.main.run_limitless_batch")
    def test_run_batch_can_run_selected_subset(
        self,
        mock_limitless,
        mock_hareruya,
        mock_ebay,
    ) -> None:
        main.run_batch(
            extract_date="2026-04-26",
            overwrite_card_index=False,
            sources=["hareruya", "ebay"],
        )

        mock_limitless.assert_not_called()
        mock_hareruya.assert_called_once_with(
            extract_date="2026-04-26",
            overwrite_card_index=False,
        )
        mock_ebay.assert_called_once_with(
            extract_date="2026-04-26",
            overwrite_card_index=False,
        )


if __name__ == "__main__":
    unittest.main()
