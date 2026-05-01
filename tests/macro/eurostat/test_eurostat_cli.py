"""
Unit tests for Eurostat command line interface

Credentials (POSTGRES_*) are loaded from .env, no CLI switches.
dbname equals module name: eurostat.
"""

import importlib.util
import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

project_root = Path(__file__).parent.parent.parent.parent.parent
src = project_root / 'src'
eurostat_dir = src / 'macro' / 'eurostat'
for p in (src, eurostat_dir):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

# Mock sdmx so we can import eurostat modules without hitting the network
_mock_sdmx = MagicMock()
sys.modules['sdmx'] = _mock_sdmx


def _load_eurostat_main():
    """Load eurostat.main without pulling in macro.__init__ (IMF/duckdb)."""
    eurostat_py = src / 'macro' / 'eurostat' / 'eurostat.py'
    spec = importlib.util.spec_from_file_location('eurostat_main', eurostat_py)
    mod = importlib.util.module_from_spec(spec)
    sys.modules['eurostat_main'] = mod
    spec.loader.exec_module(mod)
    return mod.main


class TestEurostatCLIArguments:
    """Tests for Eurostat CLI - no db switches, credentials from env"""

    def test_no_db_switches_in_help(self):
        """Eurostat CLI has no --dbuser, --dbhost, --dbport, --dbpassword in help."""
        from io import StringIO

        eurostat_main = _load_eurostat_main()

        with patch('sys.argv', ['eurostat.py', '--help']):
            with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                try:
                    eurostat_main()
                except SystemExit:
                    pass
        help_text = mock_stdout.getvalue()
        assert '--dbuser' not in help_text
        assert '--dbhost' not in help_text
        assert '--dbport' not in help_text
        assert '--dbpassword' not in help_text

    def test_eurostat_downloader_called_without_db_args(self):
        """EurostatDataDownloader is instantiated with no user/host/password/port."""
        # Patch before loading - eurostat_main imports EurostatDataDownloader in main()
        with patch('eurostat_data_downloader.EurostatDataDownloader') as mock_dl:
            eurostat_main = _load_eurostat_main()
            mock_dl.return_value.get_all_downloadable_series = MagicMock(return_value=[])

            env = {
                'POSTGRES_USER': 'testuser',
                'POSTGRES_HOST': 'localhost',
                'POSTGRES_PASSWORD': 'testpass',
                'POSTGRES_PORT': '5432',
            }
            with patch.dict('os.environ', env, clear=False):
                with patch('sys.argv', ['eurostat.py', '--generate-catalog']):
                    with patch('eurostat_main.load_environment_config'):
                        eurostat_main()

                    mock_dl.assert_called_once_with()
                    assert mock_dl.call_args[1] == {}

    def test_database_connection_uses_module_dbname_and_env(self):
        """EurostatDataDownloader uses dbname='eurostat' and creds from env."""
        from eurostat_data_downloader import EurostatDataDownloader

        env = {
            'POSTGRES_USER': 'testuser',
            'POSTGRES_HOST': 'testhost',
            'POSTGRES_PASSWORD': 'testpass',
            'POSTGRES_PORT': '5432',
        }
        with patch.dict('os.environ', env, clear=False):
            d = EurostatDataDownloader()
            assert d.dbname == 'eurostat'
            assert d.user == 'testuser'
            assert d.host == 'testhost'
            assert d.password == 'testpass'
            assert d.port == 5432

    def test_missing_postgres_env_raises(self):
        """EurostatDataDownloader raises when POSTGRES_* env vars are missing."""
        import os
        from eurostat_data_downloader import EurostatDataDownloader

        env_no_postgres = {k: v for k, v in os.environ.items()
                          if not k.startswith('POSTGRES_')}
        with patch.dict(os.environ, env_no_postgres, clear=True):
            with pytest.raises(ValueError, match='POSTGRES_'):
                EurostatDataDownloader()
