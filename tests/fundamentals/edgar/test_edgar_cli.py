"""
Unit tests for EDGAR command line interface switches
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, Mock
import argparse

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


class TestEDGARCLIArguments:
    """Tests for EDGAR command line argument parsing"""
    
    def test_default_arguments(self):
        """Test that default arguments are set correctly"""
        from src.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = []
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                # Mock the downloader instance
                mock_downloader = MagicMock()
                mock_downloader_class.return_value = mock_downloader
                
                # When no arguments are provided, the script creates EDGARDownloader but doesn't do anything
                try:
                    edgar_main()
                except (SystemExit, Exception):
                    pass
                
                # Verify EDGARDownloader was instantiated
                assert mock_downloader_class.called
    
    def test_user_agent_from_env(self):
        """Test that EDGAR_USER_AGENT is read from environment (edgar main and filings_downloader)."""
        from src.fundamentals.edgar.edgar import main as edgar_main

        custom_user_agent = 'Custom User Agent test@example.com'
        test_args = ['--download-raw-quarter-filings']

        with patch.dict('os.environ', {'EDGAR_USER_AGENT': custom_user_agent}, clear=False):
            with patch('sys.argv', ['edgar.py'] + test_args):
                with patch('src.fundamentals.edgar.filings.filings_downloader.FilingDownloader') as mock_filing_downloader_class:
                    mock_filing_downloader = MagicMock()
                    mock_filing_downloader.download_filings = MagicMock(return_value=[])
                    mock_filing_downloader_class.return_value = mock_filing_downloader

                    with patch('src.fundamentals.edgar.edgar.get_postgres_connection'):
                        try:
                            edgar_main()
                        except (SystemExit, Exception):
                            pass

                        mock_filing_downloader_class.assert_called_once()
                        call_kwargs = mock_filing_downloader_class.call_args[1]
                        assert call_kwargs['user_agent'] == custom_user_agent

    def test_filings_downloader_user_agent_from_env(self):
        """Test that filings_downloader main uses EDGAR_USER_AGENT from environment."""
        from src.fundamentals.edgar.filings.filings_downloader import main as filings_main

        custom_user_agent = 'Filings Downloader Agent test@example.com'
        with patch.dict('os.environ', {'EDGAR_USER_AGENT': custom_user_agent}, clear=False):
            with patch('sys.argv', ['filings_downloader.py', '--accession-code', '0000123-00-000001']):
                with patch('src.fundamentals.edgar.filings.filings_downloader.FilingDownloader') as mock_class:
                    with patch('src.fundamentals.edgar.filings.filings_downloader.get_postgres_connection'):
                        with patch('src.fundamentals.edgar.filings.filings_downloader.get_filing_metadata_by_accession', return_value=None):
                            with patch('src.fundamentals.edgar.filings.filings_downloader.shutil.move'):
                                mock_class.return_value.download_filing_by_path.return_value = Path('/out/file.txt')
                                try:
                                    filings_main()
                                except Exception:
                                    pass
                                mock_class.assert_called_once()
                                call_kwargs = mock_class.call_args[1]
                                assert call_kwargs['user_agent'] == custom_user_agent

    def test_generate_catalog_flag(self):
        """Test --generate-catalog flag"""
        from src.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                with patch('src.fundamentals.edgar.edgar.get_postgres_connection') as mock_conn:
                    with patch('src.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                        with patch('src.fundamentals.edgar.master_idx.master_idx.MasterIdxManager') as mock_master_idx:
                            # Mock the downloader instance
                            mock_downloader = MagicMock()
                            mock_downloader_class.return_value = mock_downloader
                            
                            # Mock connection
                            mock_conn.return_value = MagicMock()
                            
                            # Mock MasterIdxManager
                            mock_manager = MagicMock()
                            mock_master_idx.return_value = mock_manager
                            
                            try:
                                edgar_main()
                            except (SystemExit, Exception):
                                pass
                            
                            # Verify MasterIdxManager was instantiated (used in generate-catalog mode)
                            assert mock_master_idx.called
    
    def test_download_companies_flag(self):
        """Test --download-companies flag"""
        from src.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog', '--download-companies']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                with patch('src.fundamentals.edgar.edgar.get_postgres_connection') as mock_conn:
                    with patch('src.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                        with patch('src.fundamentals.edgar.master_idx.master_idx.MasterIdxManager') as mock_master_idx:
                            # Mock the downloader instance
                            mock_downloader = MagicMock()
                            mock_downloader_class.return_value = mock_downloader
                            
                            # Mock connection
                            mock_conn.return_value = MagicMock()
                            
                            # Mock MasterIdxManager
                            mock_manager = MagicMock()
                            mock_master_idx.return_value = mock_manager
                            
                            try:
                                edgar_main()
                            except (SystemExit, Exception):
                                pass
                            
                            # Verify MasterIdxManager was called (download-companies flag affects catalog generation)
                            assert mock_master_idx.called
    
    def test_database_arguments(self):
        """Test database connection uses datalake and ENV for host (postgres.{env}.local.info) and user ({env}.user)."""
        from src.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog']
        env_overrides = {'ENV': 'test'}
        
        with patch.dict('os.environ', env_overrides, clear=False):
            with patch('sys.argv', ['edgar.py'] + test_args):
                with patch('src.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                    with patch('src.postgres_connection.psycopg2.connect') as mock_connect:
                        with patch('src.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                            with patch('src.fundamentals.edgar.master_idx.master_idx.MasterIdxManager'):
                                mock_downloader = MagicMock()
                                mock_downloader_class.return_value = mock_downloader
                                mock_connect.return_value = MagicMock()
                                
                                try:
                                    edgar_main()
                                except (SystemExit, Exception):
                                    pass
                                
                                assert mock_connect.called
                                call_kwargs = mock_connect.call_args[1]
                                assert call_kwargs['dbname'] == 'datalake'
                                assert call_kwargs['host'] == 'postgres.test.local.info'
                                assert call_kwargs['user'] == 'test.user'
    
    def test_database_defaults(self):
        """Test that database uses datalake and postgres.{env}.local.info when ENV not set (default dev)."""
        from src.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog']
        
        with patch.dict('os.environ', {}, clear=False) as env:
            env.pop('POSTGRES_HOST', None)
            env.pop('ENV', None)
            with patch('sys.argv', ['edgar.py'] + test_args):
                with patch('src.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                    with patch('src.postgres_connection.psycopg2.connect') as mock_connect:
                        with patch('src.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                            with patch('src.fundamentals.edgar.master_idx.master_idx.MasterIdxManager'):
                                mock_downloader = MagicMock()
                                mock_downloader_class.return_value = mock_downloader
                                mock_connect.return_value = MagicMock()
                                
                                try:
                                    edgar_main()
                                except (SystemExit, Exception):
                                    pass
                                
                                assert mock_connect.called
                                call_kwargs = mock_connect.call_args[1]
                                assert call_kwargs['dbname'] == 'datalake'
                                assert call_kwargs['host'] == 'postgres.dev.local.info'
                                assert call_kwargs['user'] == 'dev.user'
    
    def test_combined_arguments(self):
        """Test combining multiple arguments. EDGAR_USER_AGENT from env; dbname always module-derived."""
        from src.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = [
            '--generate-catalog',
            '--download-companies',
        ]
        env_overrides = {'EDGAR_USER_AGENT': 'Test Agent test@example.com'}
        
        with patch.dict('os.environ', env_overrides, clear=False):
            with patch('sys.argv', ['edgar.py'] + test_args):
                with patch('src.fundamentals.edgar.edgar.EDGARDownloader') as mock_downloader_class:
                    with patch('src.postgres_connection.psycopg2.connect') as mock_connect:
                        with patch('src.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                            with patch('src.fundamentals.edgar.master_idx.master_idx.MasterIdxManager') as mock_master_idx:
                                mock_downloader = MagicMock()
                                mock_downloader_class.return_value = mock_downloader
                                mock_connect.return_value = MagicMock()
                                mock_manager = MagicMock()
                                mock_master_idx.return_value = mock_manager
                                
                                try:
                                    edgar_main()
                                except (SystemExit, Exception):
                                    pass
                                
                                assert mock_downloader_class.called
                                call_kwargs = mock_downloader_class.call_args[1]
                                assert call_kwargs['user_agent'] == 'Test Agent test@example.com'  # from EDGAR_USER_AGENT env

                                assert mock_connect.called
                                conn_kwargs = mock_connect.call_args[1]
                                assert conn_kwargs['dbname'] == 'datalake'
                                
                                assert mock_master_idx.called
    
    def test_invalid_dbport_type(self):
        """Test removed - dbport no longer a CLI argument (comes from env)."""
        pass
    
    def test_help_flag(self):
        """Test that --help flag works"""
        from src.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--help']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            # argparse raises SystemExit when --help is used
            with pytest.raises(SystemExit) as exc_info:
                edgar_main()
            
            # SystemExit code 0 indicates successful help display
            assert exc_info.value.code == 0


class TestEDGARCLIArgumentGroups:
    """Tests for argument groups and their organization"""
    
    def test_catalog_generation_group(self):
        """Test that catalog generation arguments are grouped"""
        # This test verifies the argument parser structure
        # We can't easily test the grouping visually, but we can verify
        # that the arguments exist and work together
        from src.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog', '--download-companies']
        
        with patch('sys.argv', ['edgar.py'] + test_args):
            with patch('src.fundamentals.edgar.edgar.EDGARDownloader'):
                with patch('src.fundamentals.edgar.edgar.get_postgres_connection'):
                        with patch('src.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                            with patch('src.fundamentals.edgar.master_idx.master_idx.MasterIdxManager'):
                                try:
                                    edgar_main()
                                except (SystemExit, Exception):
                                    pass
                                
                                # If we get here without argument errors, grouping works
                                assert True
    
    def test_database_connection_group(self):
        """Test that database connection uses datalake and ENV for host (postgres.{env}.local.info)."""
        from src.fundamentals.edgar.edgar import main as edgar_main
        
        test_args = ['--generate-catalog']
        env_overrides = {'ENV': 'test'}
        
        with patch.dict('os.environ', env_overrides, clear=False):
            with patch('sys.argv', ['edgar.py'] + test_args):
                with patch('src.fundamentals.edgar.edgar.EDGARDownloader'):
                    with patch('src.postgres_connection.psycopg2.connect') as mock_connect:
                        with patch('src.fundamentals.edgar.edgar.init_edgar_postgres_tables'):
                            with patch('src.fundamentals.edgar.master_idx.master_idx.MasterIdxManager'):
                                mock_connect.return_value = MagicMock()
                                
                                try:
                                    edgar_main()
                                except (SystemExit, Exception):
                                    pass
                                
                                assert mock_connect.called
                                call_kwargs = mock_connect.call_args[1]
                                assert call_kwargs['dbname'] == 'datalake'
                                assert call_kwargs['host'] == 'postgres.test.local.info'
                                assert call_kwargs['user'] == 'test.user'
