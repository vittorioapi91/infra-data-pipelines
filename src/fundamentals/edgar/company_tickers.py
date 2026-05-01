"""
SEC EDGAR Companies Data Management

This module handles downloading and managing company data from SEC EDGAR,
including the company_tickers.json file.
"""

import logging
import os
from pathlib import Path
from typing import Optional
import requests

logger = logging.getLogger(__name__)


class CompaniesDownloader:
    """Class to download company data from SEC EDGAR"""
    
    def __init__(self, user_agent: str = "VittorioApicella apicellavittorio@hotmail.it"):
        """
        Initialize Companies Downloader
        
        Args:
            user_agent: User-Agent string for SEC EDGAR requests (required by SEC)
        """
        self.user_agent = user_agent
        self.base_url = "https://www.sec.gov"
        self.headers = {
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'www.sec.gov'
        }
        
        # Set up edgar root directory for storing files
        edgar_dir = Path(__file__).parent
        self.edgar_root = edgar_dir
    
    def download_company_tickers_json(self, output_dir: Optional[Path] = None) -> Path:
        """
        Download company_tickers.json from SEC EDGAR and save it raw to disk.
        
        The company_tickers.json file contains a mapping of all companies with their CIK, ticker, and name.
        Endpoint: https://www.sec.gov/files/company_tickers.json
        
        Args:
            output_dir: Optional directory to save the file. If not provided, saves to edgar root.
        
        Returns:
            Path to the saved company_tickers.json file
            
        Raises:
            Exception: If download fails
        """
        logger.info("Downloading company_tickers.json from SEC EDGAR...")
        
        # SEC EDGAR company_tickers.json endpoint
        companies_url = f"{self.base_url}/files/company_tickers.json"
        
        try:
            response = requests.get(companies_url, headers=self.headers, timeout=30)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            # Determine output directory
            if output_dir is None:
                output_dir = self.edgar_root
            else:
                output_dir = Path(output_dir)
                output_dir.mkdir(parents=True, exist_ok=True)
            
            # Save to output directory with the same filename as on the server
            companies_file = output_dir / "company_tickers.json"
            
            # Write raw content to disk
            with open(companies_file, 'wb') as f:
                f.write(response.content)
            
            file_size = companies_file.stat().st_size
            logger.info("Successfully downloaded company_tickers.json to %s", companies_file)
            logger.info("File size: %s bytes (%.2f KB)", file_size, file_size / 1024)
            return companies_file
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to download company_tickers.json: {e}"
            logger.error("%s", error_msg)
            raise Exception(error_msg) from e

    def download_company_tickers_exchange_json(self, output_dir: Optional[Path] = None) -> Path:
        """
        Download company_tickers_exchange.json from SEC EDGAR.
        Contains CIK, ticker, title, and exchange (e.g. NASDAQ, NYSE).
        Endpoint: https://www.sec.gov/files/company_tickers_exchange.json
        """
        logger.info("Downloading company_tickers_exchange.json from SEC EDGAR...")
        url = f"{self.base_url}/files/company_tickers_exchange.json"
        try:
            response = requests.get(url, headers=self.headers, timeout=60)
            response.raise_for_status()
            output_dir = Path(output_dir) if output_dir else self.edgar_root
            output_dir.mkdir(parents=True, exist_ok=True)
            out_file = output_dir / "company_tickers_exchange.json"
            with open(out_file, "wb") as f:
                f.write(response.content)
            logger.info("Downloaded %s (%s bytes)", out_file, len(response.content))
            return out_file
        except requests.exceptions.RequestException as e:
            logger.error("Failed to download company_tickers_exchange.json: %s", e)
            raise Exception(f"Failed to download company_tickers_exchange.json: {e}") from e
