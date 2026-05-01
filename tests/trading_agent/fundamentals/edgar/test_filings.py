"""
Unit tests for EDGAR filings download functionality
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.fundamentals.edgar.filings.filings_downloader import FilingDownloader
import requests


class TestFilingDownloader:
    """Tests for FilingDownloader class"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests"""
        temp_path = Path(tempfile.mkdtemp())
        yield temp_path
        shutil.rmtree(temp_path)
    
    @pytest.fixture
    def downloader(self, temp_dir):
        """Create FilingDownloader with temporary output directory"""
        downloader = FilingDownloader()
        return downloader
    
    def test_download_filing_by_path_success(self, downloader, temp_dir):
        """Test successful download of a filing by path"""
        # Mock requests.get to return a successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"Test filing content"
        mock_response.raise_for_status = Mock()
        
        with patch('src.fundamentals.edgar.filings.filings_downloader.requests.get', return_value=mock_response):
            output_file = downloader.download_filing_by_path(
                filing_path="edgar/data/315293/0001179110-05-003398.txt",
                output_dir=str(temp_dir)
            )
            
            assert output_file.exists()
            assert output_file.name == "0001179110-05-003398.txt"
            assert output_file.read_bytes() == b"Test filing content"
    
    def test_download_filing_by_path_not_found(self, downloader, temp_dir):
        """Test handling of 404 error when filing is not found"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        with patch('src.fundamentals.edgar.filings.filings_downloader.requests.get', return_value=mock_response):
            with pytest.raises(FileNotFoundError):
                downloader.download_filing_by_path(
                    filing_path="edgar/data/315293/nonexistent.txt",
                    output_dir=str(temp_dir)
                )
    
    @patch('src.fundamentals.edgar.filings.filings_downloader.get_postgres_connection')
    def test_download_filings_by_year_only(self, mock_get_conn, downloader, temp_dir):
        """Test downloading filings by year only (2005, LIMIT 100)"""
        # Mock database connection
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        
        # Mock query results - return exactly 100 filenames
        mock_filenames = [("edgar/data/1000045/0000950170-05-%06d.txt" % i,) for i in range(1, 101)]
        mock_cur.fetchall.return_value = mock_filenames
        mock_get_conn.return_value = mock_conn
        
        # Mock successful download responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"Test filing content"
        mock_response.raise_for_status = Mock()
        
        with patch('src.fundamentals.edgar.filings.filings_downloader.requests.get', return_value=mock_response):
            downloaded_files = downloader.download_filings(
                year=2005,
                limit=100,
                output_dir=str(temp_dir)
            )
            
            # Verify LIMIT was included in query
            execute_calls = mock_cur.execute.call_args_list
            # Check if any query includes LIMIT clause
            limit_found = False
            limit_value = None
            for call in execute_calls:
                query = call[0][0] if call[0] else ""
                params = call[0][1] if len(call[0]) > 1 else []
                if "LIMIT" in query.upper():
                    limit_found = True
                    # Check if 100 is in the params (parameterized query) or in the query string
                    if 100 in params or "100" in query:
                        limit_value = 100
                        break
            
            assert limit_found, "Query should include LIMIT clause"
            assert limit_value == 100, f"Query should include LIMIT 100, but found {limit_value}"
            
            # Verify exactly 100 files were downloaded
            assert len(downloaded_files) == 100
    
    @patch('src.fundamentals.edgar.filings.filings_downloader.get_postgres_connection')
    def test_download_filings_by_year_quarter_form_type(self, mock_get_conn, downloader, temp_dir):
        """Test downloading filings by year, quarter, and form_type"""
        # Mock database connection
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        
        # Mock query results - return 5 filenames
        mock_filenames = [
            ("edgar/data/1000045/0000950170-05-000001.txt",),
            ("edgar/data/1000045/0000950170-05-000002.txt",),
        ]
        mock_cur.fetchall.return_value = mock_filenames
        mock_get_conn.return_value = mock_conn
        
        # Mock successful download responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"Test filing content"
        mock_response.raise_for_status = Mock()
        
        with patch('src.fundamentals.edgar.filings.filings_downloader.requests.get', return_value=mock_response):
            downloaded_files = downloader.download_filings(
                year=2005,
                quarter="QTR1",
                form_type="10-K",
                limit=100,
                output_dir=str(temp_dir)
            )
            
            # Verify query included all filters
            execute_calls = [call[0][0] for call in mock_cur.execute.call_args_list]
            query_str = str(execute_calls)
            assert "year" in query_str.lower()
            assert "quarter" in query_str.lower()
            assert "form_type" in query_str.lower()
            
            assert len(downloaded_files) == 2
    
    @patch('src.fundamentals.edgar.filings.filings_downloader.get_postgres_connection')
    def test_download_filings_by_cik(self, mock_get_conn, downloader, temp_dir):
        """Test downloading filings by CIK only"""
        # Mock database connection
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        
        # Mock query results
        mock_filenames = [
            ("edgar/data/315293/0001179110-05-003398.txt",),
        ]
        mock_cur.fetchall.return_value = mock_filenames
        mock_get_conn.return_value = mock_conn
        
        # Mock successful download responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"Test filing content"
        mock_response.raise_for_status = Mock()
        
        with patch('src.fundamentals.edgar.filings.filings_downloader.requests.get', return_value=mock_response):
            downloaded_files = downloader.download_filings(
                cik='315293',
                output_dir=str(temp_dir)
            )
            
            # Verify CIK filter was used (should normalize to 10 digits)
            execute_calls = [call[0][0] for call in mock_cur.execute.call_args_list]
            query_str = str(execute_calls)
            assert "cik" in query_str.lower()
            
            assert len(downloaded_files) == 1
    
    @patch('src.fundamentals.edgar.filings.filings_downloader.get_postgres_connection')
    def test_download_filings_by_filename(self, mock_get_conn, downloader, temp_dir):
        """Test downloading filings by filename only"""
        # Mock database connection
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        
        # Mock query results
        mock_filenames = [
            ("edgar/data/315293/0001179110-05-003398.txt",),
        ]
        mock_cur.fetchall.return_value = mock_filenames
        mock_get_conn.return_value = mock_conn
        
        # Mock successful download responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"Test filing content"
        mock_response.raise_for_status = Mock()
        
        with patch('src.fundamentals.edgar.filings.filings_downloader.requests.get', return_value=mock_response):
            downloaded_files = downloader.download_filings(
                filename='edgar/data/315293/0001179110-05-003398.txt',
                output_dir=str(temp_dir)
            )
            
            assert len(downloaded_files) == 1
    
    @patch('src.fundamentals.edgar.filings.filings_downloader.get_postgres_connection')
    def test_download_filings_no_filters_raises_error(self, mock_get_conn, downloader, temp_dir):
        """Test that providing no filters raises an error"""
        with pytest.raises(ValueError, match="At least one filter must be provided"):
            downloader.download_filings(output_dir=str(temp_dir))
    
    @patch('src.fundamentals.edgar.filings.filings_downloader.get_postgres_connection')
    def test_download_filings_invalid_filter_raises_error(self, mock_get_conn, downloader, temp_dir):
        """Test that invalid filter names raise an error"""
        with pytest.raises(ValueError, match="Unknown filter"):
            downloader.download_filings(invalid_filter='value', output_dir=str(temp_dir))
    
    @patch('src.fundamentals.edgar.filings.filings_downloader.get_postgres_connection')
    def test_download_filings_no_results(self, mock_get_conn, downloader, temp_dir):
        """Test handling when no filings are found - should raise ValueError"""
        # Mock database connection
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        
        # Mock empty results
        mock_cur.fetchall.return_value = []
        mock_get_conn.return_value = mock_conn
        
        # Should raise ValueError when no filings are found
        with pytest.raises(ValueError, match="No filings found for filters"):
            downloader.download_filings(
                year=2005,
                quarter="QTR1",
                form_type="10-K",
                limit=100,
                output_dir=str(temp_dir)
            )
    
    @patch('src.fundamentals.edgar.filings.filings_downloader.get_postgres_connection')
    def test_download_filings_handles_download_errors(self, mock_get_conn, downloader, temp_dir):
        """Test that download errors don't stop the entire process"""
        # Mock database connection
        mock_conn = Mock()
        mock_cur = Mock()
        mock_conn.cursor.return_value = mock_cur
        
        # Mock query results - return 3 filenames
        mock_filenames = [
            ("edgar/data/1000045/0000950170-05-000001.txt",),
            ("edgar/data/1000045/0000950170-05-000002.txt",),
            ("edgar/data/1000045/0000950170-05-000003.txt",),
        ]
        mock_cur.fetchall.return_value = mock_filenames
        mock_get_conn.return_value = mock_conn
        
        # Mock mixed responses - first succeeds, second fails, third succeeds
        responses = [
            Mock(status_code=200, content=b"Success 1", raise_for_status=Mock()),
            Mock(status_code=404, raise_for_status=Mock(side_effect=requests.exceptions.HTTPError(response=Mock(status_code=404)))),
            Mock(status_code=200, content=b"Success 3", raise_for_status=Mock()),
        ]
        
        with patch('src.fundamentals.edgar.filings.filings_downloader.requests.get', side_effect=responses):
            with patch('builtins.print'):  # Suppress print output
                downloaded_files = downloader.download_filings(
                    year=2005,
                    quarter="QTR1",
                    form_type="10-K",
                    limit=100,
                    output_dir=str(temp_dir)
                )
                
                # Should have downloaded 2 out of 3 files (skipping the failed one)
                assert len(downloaded_files) == 2
