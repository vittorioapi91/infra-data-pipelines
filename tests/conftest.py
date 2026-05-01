"""
Pytest configuration - sets env vars for testing (e.g. ISHARES_HOLDINGS_LIMIT to limit holdings download).
"""

import os


def pytest_configure(config):
    """Set ISHARES_HOLDINGS_LIMIT when running tests to limit holdings download."""
    if "ISHARES_HOLDINGS_LIMIT" not in os.environ:
        os.environ["ISHARES_HOLDINGS_LIMIT"] = "2"
