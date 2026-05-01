"""Macro economic model module for downloading economic data"""

from .imf.imf_data_downloader import IMFDataDownloader

try:
    from .fred.fred_data_downloader import FREDDataDownloader
    from .bis.bis_data_downloader import BISDataDownloader
except ImportError:
    from src.macro.fred.fred_data_downloader import FREDDataDownloader
    from src.macro.bis.bis_data_downloader import BISDataDownloader

__all__ = ['IMFDataDownloader', 'FREDDataDownloader', 'BISDataDownloader']
