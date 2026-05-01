"""
Main entry point for TradingPythonAgent.

Run from repo root: python scripts/main.py
"""

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Load environment configuration early
import src.config  # noqa: F401

from src.agent import TradingAgent


def main():
    """Main function to run the trading agent"""
    agent = TradingAgent()
    agent.run()


if __name__ == "__main__":
    main()
