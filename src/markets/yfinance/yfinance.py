"""
Main entry point for Yahoo Finance Equities Data Downloader

This module provides a command-line interface to download equity data from Yahoo Finance.
--generate-catalog: upsert unified symbols table in yfinance DB from nasdaqtrader.nasdaqlisted + otherlisted.
--update-metadata: download selected data types (at least one type switch required) to storage JSON.
"""

import logging
import os
from pathlib import Path
from typing import Optional, Set

from .yahoo_equities_downloader import YahooEquitiesDownloader

logger = logging.getLogger(__name__)

# One per yfinance Ticker attribute / lump; .info is equity_info (valuation etc. live there)
YFINANCE_DATA_TYPES = (
    "equity_info",           # .info
    "analyst_recommendations",  # .recommendations + .upgrades_downgrades
    "eps_revisions",         # .eps_revisions
    "revenue_estimates",     # .revenue_estimate
    "analyst_price_targets", # .analyst_price_targets
    "earnings_calendar",     # .earnings_dates
    "earnings_history",      # .earnings_history
    "financial_statements",  # .income_stmt, .balance_sheet, .cashflow (+ quarterly)
    "calendar",              # .calendar
    "sec_filings",           # .sec_filings
)


def _run_generate_catalog() -> None:
    """Ensure nasdaqtrader is populated (download + upsert), then concatenate nasdaqlisted + otherlisted and upsert into yfinance.symbols. Fails if yfinance DB does not exist; creates symbols table if missing."""
    import sys
    from tqdm import tqdm

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        from src.config import load_environment_config
        load_environment_config()
    except Exception:
        if not os.getenv("POSTGRES_PASSWORD"):
            from dotenv import load_dotenv
            load_dotenv(project_root / ".env.dev", override=True)

    from src.markets.nasdaqtrader.nasdaqtrader_downloader import download_symbol_directory
    from src.markets.nasdaqtrader.nasdaqtrader_postgres import (
        exchanges_table_exists,
        get_postgres_connection as get_nasdaqtrader_conn,
        init_nasdaqtrader_postgres_tables,
        upload_exchanges_reference_data,
        upload_symbol_directory_to_db,
    )
    from .yfinance_postgres import (
        get_postgres_connection as get_yfinance_conn,
        init_yfinance_postgres_tables,
        generate_catalog_from_nasdaqtrader,
    )

    steps = [
        "Download symbol directory",
        "Upload to nasdaqtrader DB",
        "Ensure exchanges table",
        "Generate yfinance.symbols",
    ]
    with tqdm(total=len(steps), desc="Generate catalog", unit="step") as pbar:
        # 1. Populate nasdaqtrader: download .txt files then upsert into nasdaqlisted + otherlisted
        pbar.set_postfix_str(steps[0], refresh=True)
        nasdaqtrader_dir = project_root / "storage" / "dev" / "markets" / "nasdaqtrader"
        nasdaqtrader_dir.mkdir(parents=True, exist_ok=True)
        download_symbol_directory(output_dir=str(nasdaqtrader_dir))
        pbar.update(1)

        pbar.set_postfix_str(steps[1], refresh=True)
        upload_symbol_directory_to_db(nasdaqtrader_dir, load_env=False)
        pbar.update(1)

        pbar.set_postfix_str(steps[2], refresh=True)
        conn_nt = get_nasdaqtrader_conn()
        if not exchanges_table_exists(conn_nt):
            init_nasdaqtrader_postgres_tables(conn_nt)
            upload_exchanges_reference_data(conn_nt)
        pbar.update(1)

        pbar.set_postfix_str(steps[3], refresh=True)
        try:
            conn_yf = get_yfinance_conn()
        except Exception as conn_err:
            conn_nt.close()
            import psycopg2
            if isinstance(conn_err, psycopg2.OperationalError):
                raise RuntimeError(
                    "Datalake connection failed or yfinance schema missing. Check ENV, POSTGRES_PASSWORD, and that the datalake database exists."
                ) from conn_err
            raise
        init_yfinance_postgres_tables(conn_yf)
        n = generate_catalog_from_nasdaqtrader(conn_nt, conn_yf)
        conn_nt.close()
        conn_yf.close()
        pbar.update(1)

    logger.info("Generate catalog: upserted %s rows into yfinance.symbols", n)


def _run_update_metadata(requested_types: Set[str], limit: Optional[int] = None) -> None:
    """Download requested data types to storage/dev/markets/yfinance/<type>/ (one JSON file per symbol per type)."""
    import json
    import logging as log_module
    import sys
    from tqdm import tqdm

    # Suppress per-error lines; we write to error log and show count only
    for _logger in ("yfinance", "urllib3.connectionpool", "src.markets.yfinance.yahoo_equities_downloader"):
        log_module.getLogger(_logger).setLevel(log_module.CRITICAL)

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        from src.config import load_environment_config
        load_environment_config()
    except Exception:
        if not os.getenv("POSTGRES_PASSWORD"):
            from dotenv import load_dotenv
            load_dotenv(project_root / ".env.dev", override=True)

    if limit is None:
        limit_str = os.getenv("YFINANCE_UPDATE_METADATA_LIMIT")
        limit = int(limit_str) if limit_str else None

    from .yahoo_equities_downloader import YahooEquitiesDownloader
    from .yfinance_postgres import (
        _to_json_safe,
        get_postgres_connection as get_yfinance_conn,
        get_symbols_from_catalog,
        init_yfinance_postgres_tables,
    )

    base_dir = project_root / "storage" / "dev" / "markets" / "yfinance"
    for sub in YFINANCE_DATA_TYPES:
        (base_dir / sub).mkdir(parents=True, exist_ok=True)

    conn = get_yfinance_conn()
    init_yfinance_postgres_tables(conn)
    symbols = get_symbols_from_catalog(conn, limit=limit)
    if not symbols:
        logger.warning("No symbols in yfinance.symbols. Run --generate-catalog first.")
        conn.close()
        return

    def _safe_filename(s: str) -> str:
        return s.replace("/", "_").replace("\\", "_") or "unknown"

    error_log_path = base_dir / "update_metadata_errors.log"
    downloader = YahooEquitiesDownloader(delay_between_requests=0.2)
    method_prefix = "get_"
    ok_by_type = {t: 0 for t in requested_types}
    failed_symbols: list[str] = []
    error_count = 0

    with open(error_log_path, "w", encoding="utf-8") as error_log:
        error_log.write("symbol\tdata_type\terror\n")
        for data_type in requested_types:
            method_name = method_prefix + data_type
            if not hasattr(downloader, method_name):
                logger.warning("Downloader has no method %s; skipping %s", method_name, data_type)
                continue
            method = getattr(downloader, method_name)
            out_dir = base_dir / data_type
            for symbol in tqdm(symbols, desc=f"Download {data_type}", unit="ticker"):
                try:
                    payload = method(symbol)
                    if payload is None and data_type != "equity_info":
                        continue
                    if payload is not None:
                        if data_type == "equity_info":
                            payload = _to_json_safe(payload)
                        path = out_dir / f"{_safe_filename(symbol)}.json"
                        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                        ok_by_type[data_type] += 1
                except Exception as e:
                    failed_symbols.append(symbol)
                    error_count += 1
                    error_log.write(f"{symbol}\t{data_type}\t{type(e).__name__}: {e}\n")
                    error_log.flush()

    conn.close()
    total_ok = sum(ok_by_type.values())
    if failed_symbols:
        failed_path = base_dir / "failed_symbols.txt"
        failed_path.write_text("\n".join(failed_symbols), encoding="utf-8")
        logger.warning(
            "Update metadata: %s total ok, %s errors (see %s). Failed symbols in %s. JSON under %s",
            total_ok, error_count, error_log_path, failed_path, base_dir,
        )
    else:
        logger.info("Update metadata: %s total ok. JSON under %s. Per-type: %s", total_ok, base_dir, ok_by_type)


def _run_download_timeseries(
    ticker_list: list[str],
    output_subdir: str,
    desc: str,
) -> None:
    """Download OHLCV timeseries for given tickers. Saves to storage/.../yfinance/timeseries/{output_subdir}/."""
    import logging as log_module
    import sys
    from pathlib import Path
    from tqdm import tqdm

    for _logger in ("yfinance", "urllib3.connectionpool"):
        log_module.getLogger(_logger).setLevel(log_module.CRITICAL)

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        from src.config import load_environment_config
        load_environment_config()
    except Exception:
        if not os.getenv("POSTGRES_PASSWORD"):
            from dotenv import load_dotenv
            load_dotenv(project_root / ".env.dev", override=True)

    storage_env = os.getenv("ENV", "dev")
    base_dir = project_root / "storage" / storage_env / "markets" / "yfinance" / "timeseries" / output_subdir
    base_dir.mkdir(parents=True, exist_ok=True)

    from .yahoo_downloader_base import YahooDownloaderBase

    def _safe_filename(s: str) -> str:
        return s.replace("/", "_").replace("\\", "_") or "unknown"

    downloader = YahooDownloaderBase(delay_between_requests=0.2)
    ok = 0
    for ticker in tqdm(ticker_list, desc=desc, unit="ticker"):
        try:
            df = downloader.get_history(ticker)
            if df is not None and not df.empty:
                path = base_dir / f"{_safe_filename(ticker)}.csv"
                df.to_csv(path)
                ok += 1
        except Exception as e:
            logger.warning("Failed %s: %s", ticker, e)
    logger.info("Downloaded timeseries for %s/%s tickers to %s", ok, len(ticker_list), base_dir)


def _run_download_etf_timeseries(limit: Optional[int] = None) -> None:
    """Download ETF OHLCV timeseries to yfinance DB and CSV. Tickers from ishares DB. Incremental: only fetches from last stored date onward."""
    import logging as log_module
    import sys
    from datetime import timedelta
    from pathlib import Path
    from tqdm import tqdm

    for _logger in ("yfinance", "urllib3.connectionpool", "src.markets.yfinance.yahoo_etf_downloader"):
        log_module.getLogger(_logger).setLevel(log_module.CRITICAL)

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        from src.config import load_environment_config
        load_environment_config()
    except Exception:
        if not os.getenv("POSTGRES_PASSWORD"):
            from dotenv import load_dotenv
            load_dotenv(project_root / ".env.dev", override=True)

    if limit is None:
        limit_str = os.getenv("YFINANCE_ETF_LIMIT")
        limit = int(limit_str) if limit_str else None

    from src.markets.ishares.ishares_postgres import (
        get_postgres_connection as get_ishares_conn,
        get_etf_tickers,
        init_ishares_postgres_tables,
    )
    from .yfinance_postgres import (
        get_postgres_connection as get_yfinance_conn,
        get_max_date_for_symbol,
        get_timeseries_as_dataframe,
        init_yfinance_postgres_tables,
        insert_timeseries_rows,
    )

    ishares_conn = get_ishares_conn(dbname="ishares")
    init_ishares_postgres_tables(ishares_conn)
    ticker_list = get_etf_tickers(ishares_conn, limit=limit)
    ishares_conn.close()

    if not ticker_list:
        logger.warning("No ETF tickers in ishares DB. Run ishares --download --master --upload first.")
        return

    storage_env = os.getenv("ENV", "dev")
    base_dir = project_root / "storage" / storage_env / "markets" / "yfinance" / "timeseries" / "etf"
    base_dir.mkdir(parents=True, exist_ok=True)

    yf_conn = get_yfinance_conn()
    init_yfinance_postgres_tables(yf_conn)

    from .yahoo_etf_downloader import YahooETFDownloader

    def _safe_filename(s: str) -> str:
        return s.replace("/", "_").replace("\\", "_") or "unknown"

    downloader = YahooETFDownloader(delay_between_requests=0.2)
    source = "etf"
    ok = 0
    for ticker in tqdm(ticker_list, desc="Download ETF timeseries", unit="ticker"):
        try:
            max_date = get_max_date_for_symbol(yf_conn, ticker, source)
            if max_date is not None:
                start_str = (max_date + timedelta(days=1)).strftime("%Y-%m-%d")
                df = downloader.get_history(ticker, start=start_str)
            else:
                df = downloader.get_history(ticker)
            if df is not None and not df.empty:
                insert_timeseries_rows(yf_conn, ticker, df, source)
            full_df = get_timeseries_as_dataframe(yf_conn, ticker, source)
            if full_df is not None and not full_df.empty:
                path = base_dir / f"{_safe_filename(ticker)}.csv"
                full_df.to_csv(path)
                ok += 1
        except Exception as e:
            logger.warning("Failed %s: %s", ticker, e)
    yf_conn.close()
    logger.info("Downloaded ETF timeseries for %s/%s tickers to %s", ok, len(ticker_list), base_dir)


def _run_download_equities_timeseries(limit: Optional[int] = None) -> None:
    """Download equities OHLCV timeseries to yfinance DB and CSV. Tickers from yfinance.symbols. Incremental: only fetches from last stored date onward."""
    import logging as log_module
    import sys
    from datetime import timedelta
    from pathlib import Path
    from tqdm import tqdm

    for _logger in ("yfinance", "urllib3.connectionpool", "src.markets.yfinance.yahoo_equities_downloader"):
        log_module.getLogger(_logger).setLevel(log_module.CRITICAL)

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        from src.config import load_environment_config
        load_environment_config()
    except Exception:
        if not os.getenv("POSTGRES_PASSWORD"):
            from dotenv import load_dotenv
            load_dotenv(project_root / ".env.dev", override=True)

    if limit is None:
        limit_str = os.getenv("YFINANCE_EQUITIES_LIMIT")
        limit = int(limit_str) if limit_str else None

    from .yfinance_postgres import (
        get_postgres_connection as get_yfinance_conn,
        get_max_date_for_symbol,
        get_symbols_from_catalog,
        get_timeseries_as_dataframe,
        init_yfinance_postgres_tables,
        insert_timeseries_rows,
    )

    conn = get_yfinance_conn()
    init_yfinance_postgres_tables(conn)
    ticker_list = get_symbols_from_catalog(conn, limit=limit)

    if not ticker_list:
        logger.warning("No symbols in yfinance.symbols. Run --generate-catalog first.")
        conn.close()
        return

    storage_env = os.getenv("ENV", "dev")
    base_dir = project_root / "storage" / storage_env / "markets" / "yfinance" / "timeseries" / "equities"
    base_dir.mkdir(parents=True, exist_ok=True)

    from .yahoo_equities_downloader import YahooEquitiesDownloader

    def _safe_filename(s: str) -> str:
        return s.replace("/", "_").replace("\\", "_") or "unknown"

    downloader = YahooEquitiesDownloader(delay_between_requests=0.2)
    source = "equities"
    ok = 0
    for ticker in tqdm(ticker_list, desc="Download equities timeseries", unit="ticker"):
        try:
            max_date = get_max_date_for_symbol(conn, ticker, source)
            if max_date is not None:
                start_str = (max_date + timedelta(days=1)).strftime("%Y-%m-%d")
                df = downloader.get_history(ticker, start=start_str)
            else:
                df = downloader.get_history(ticker)
            if df is not None and not df.empty:
                insert_timeseries_rows(conn, ticker, df, source)
            full_df = get_timeseries_as_dataframe(conn, ticker, source)
            if full_df is not None and not full_df.empty:
                path = base_dir / f"{_safe_filename(ticker)}.csv"
                full_df.to_csv(path)
                ok += 1
        except Exception as e:
            logger.warning("Failed %s: %s", ticker, e)
    conn.close()
    logger.info("Downloaded equities timeseries for %s/%s tickers to %s", ok, len(ticker_list), base_dir)


def _run_upload_equity_info() -> None:
    """Read equity_info JSON files from storage, apply schema.yaml (filter + convert types), upsert into yfinance.equity_info."""
    import sys

    project_root = Path(__file__).resolve().parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    try:
        from src.config import load_environment_config
        load_environment_config()
    except Exception:
        if not os.getenv("POSTGRES_PASSWORD"):
            from dotenv import load_dotenv
            load_dotenv(project_root / ".env.dev", override=True)

    from .yfinance_postgres import (
        get_postgres_connection as get_yfinance_conn,
        init_yfinance_postgres_tables,
        upload_equity_info_from_json_dir,
    )

    equity_info_dir = project_root / "storage" / "dev" / "markets" / "yfinance" / "equity_info"
    if not equity_info_dir.is_dir():
        logger.warning("Directory not found: %s. Run --update-metadata first.", equity_info_dir)
        return

    conn = get_yfinance_conn()
    init_yfinance_postgres_tables(conn)
    n = upload_equity_info_from_json_dir(conn, equity_info_dir)
    conn.close()
    logger.info("Uploaded %s equity_info rows (schema-cleaned) from %s", n, equity_info_dir)


def main():
    """Main function to download Yahoo Finance equity data"""
    import argparse

    parser = argparse.ArgumentParser(description='Download equity data and metadata from Yahoo Finance')
    parser.add_argument('--generate-catalog', action='store_true',
                       help='Upsert symbols catalog from nasdaqtrader DB (nasdaqlisted + otherlisted) into yfinance.symbols')
    parser.add_argument('--update-metadata', action='store_true',
                       help='Download selected data types to storage JSON; at least one data-type switch required')
    for name in YFINANCE_DATA_TYPES:
        flag = "--" + name.replace("_", "-")
        parser.add_argument(flag, action='store_true', dest=f"type_{name}",
                           help=f'With --update-metadata: download {name}')
    parser.add_argument('--upload-equity-info', action='store_true',
                       help='Read JSON files from storage/dev/markets/yfinance/equity_info/, apply schema (filter + convert), upsert into yfinance.equity_info')
    parser.add_argument('--download-timeseries', action='store_true',
                       help='Download OHLCV timeseries; use --etf and/or --equities')
    parser.add_argument('--etf', action='store_true',
                       help='With --download-timeseries: ETF tickers from ishares DB')
    parser.add_argument('--equities', action='store_true',
                       help='With --download-timeseries: equity tickers from yfinance.symbols')

    args = parser.parse_args()

    try:
        if args.download_timeseries and args.etf:
            _run_download_etf_timeseries(limit=None)
            return 0
        if args.download_timeseries and args.equities:
            _run_download_equities_timeseries(limit=None)
            return 0
        if args.download_timeseries:
            parser.error("--download-timeseries requires --etf and/or --equities")

        if args.generate_catalog:
            _run_generate_catalog()
            return 0

        if args.update_metadata:
            requested = {t for t in YFINANCE_DATA_TYPES if getattr(args, f"type_{t}", False)}
            if not requested:
                parser.error("--update-metadata requires at least one data-type switch (e.g. --equity-info, --analyst-recommendations, ...)")
            _run_update_metadata(requested, limit=None)
            return 0

        if args.upload_equity_info:
            _run_upload_equity_info()
            return 0

        logger.error("Specify one of: --generate-catalog, --update-metadata, --upload-equity-info, --download-timeseries (with --etf and/or --equities)")
        return 1

    except Exception as e:
        logger.exception("Error: %s", e)
        return 1


if __name__ == "__main__":
    exit(main())
