"""
FINRA fixed-income and Query API datasets.

CLI: generate catalogs under storage/{ENV}/markets/finra/ (e.g. corporate & agency bonds).
Generic download: use download_finra_dataset() for any FINRA group/dataset.
"""

from src.markets.finra.finra import download_finra_dataset

__all__ = ["download_finra_dataset"]

