"""
Environment Configuration Module

This module handles environment-specific configuration by detecting the current
git branch and loading the appropriate .env file.

Environment mapping:
- dev/* branches → .env.dev
- staging branch → .env.test
- main branch → .env.prod

PostgreSQL (datalake): connect to database "datalake", host postgres.{ENV}.local.info,
user {ENV}.user, password from POSTGRES_PASSWORD. Each former DB (edgar, ishares, fred, etc.) is a schema.
"""

import logging
import os
import subprocess

logger = logging.getLogger(__name__)
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
except ImportError:
    # If python-dotenv is not installed, provide a no-op function
    def load_dotenv(*args, **kwargs):
        pass


def get_git_branch() -> Optional[str]:
    """
    Get the current git branch name.
    
    Returns:
        Branch name or None if not in a git repository or if git command fails
    """
    try:
        # Try to get branch from git command
        result = subprocess.run(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path(__file__).resolve().parent.parent,
            timeout=5  # Add timeout to prevent hanging
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError, PermissionError, OSError, subprocess.TimeoutExpired):
        # Fallback: try to get from environment variable (useful in CI/CD or containers)
        # This handles cases where git is not available or not accessible
        return os.getenv('GIT_BRANCH') or os.getenv('BRANCH_NAME') or os.getenv('AIRFLOW_ENV')


def get_environment_from_branch(branch: Optional[str]) -> str:
    """
    Determine environment name from git branch.
    
    Args:
        branch: Git branch name
        
    Returns:
        Environment name: 'dev', 'staging', or 'prod'
    """
    if not branch:
        # Default to dev if branch cannot be determined
        return 'dev'
    
    branch_lower = branch.lower()
    
    # Check for staging branch
    if branch_lower == 'staging':
        return 'staging'
    
    # Check for main/master branch
    if branch_lower in ('main', 'master'):
        return 'prod'
    
    # Check for dev branches (dev/* or starts with dev)
    if branch_lower.startswith('dev') or '/' in branch_lower:
        # If it's a branch like dev/DEV-4, extract the prefix
        if '/' in branch_lower:
            prefix = branch_lower.split('/')[0]
            if prefix == 'dev':
                return 'dev'
    
    # Default to dev for any other branch
    return 'dev'


def get_environment() -> str:
    """
    Get the current environment name based on git branch.
    
    Returns:
        Environment name: 'dev', 'staging', or 'prod'
    """
    branch = get_git_branch()
    return get_environment_from_branch(branch)


def get_storage_root() -> Path:
    """
    Get the environment-specific storage root directory for runtime data (EDGAR, FRED, etc.).

    Uses TRADING_AGENT_STORAGE from the appropriate .env file as a common
    storage root (without env), and appends the resolved environment name.
    """
    root = os.getenv('TRADING_AGENT_STORAGE')
    if not root:
        raise ValueError(
            "TRADING_AGENT_STORAGE is not set. Add it to your .env file (e.g. .env.dev)."
        )
    env = get_environment()
    return Path(root) / env


def get_requirements_file() -> Path:
    """
    Get the path to the environment-specific requirements file.
    
    Returns:
        Path to requirements file (requirements-dev.txt, requirements-staging.txt, or requirements-prod.txt)
        Falls back to requirements.txt if environment-specific file doesn't exist
    """
    env = get_environment()
    project_root = Path(__file__).resolve().parent.parent
    
    if env == 'dev':
        req_file = project_root / 'requirements-dev.txt'
    elif env == 'staging':
        req_file = project_root / 'requirements-staging.txt'
    else:  # prod
        req_file = project_root / 'requirements-prod.txt'
    
    # Fallback to base requirements.txt if environment-specific file doesn't exist
    if not req_file.exists():
        return project_root / 'requirements.txt'
    
    return req_file


def load_environment_config(env_name: Optional[str] = None) -> None:
    """
    Load environment variables from the appropriate .env file based on branch.
    
    Args:
        env_name: Optional environment name ('dev', 'staging', 'prod').
                 If None, will be determined from git branch.
    """
    # Skip auto-loading in Airflow - trading_agent_dags.py handles env loading
    if os.getenv('AIRFLOW_HOME') is not None or '/opt/airflow' in str(Path(__file__).absolute()):
        return
    
    if env_name is None:
        branch = get_git_branch()
        env_name = get_environment_from_branch(branch)
    
    # Map env name to .env file (staging uses .env.test)
    _env_file_suffix = {'staging': 'test'}.get(env_name, env_name)
    env_filename = f'.env.{_env_file_suffix}'
    
    # Try multiple candidate project roots (config at src/config.py -> 2 parents; or cwd)
    candidates = [
        Path(__file__).resolve().parent.parent,  # src/config.py -> project root
        Path.cwd(),
    ]
    env_file = None
    project_root = None
    for root in candidates:
        candidate = root / env_filename
        if candidate.exists():
            env_file = candidate
            project_root = root
            break
    
    if env_file is None:
        raise FileNotFoundError(
            f"Environment file {env_filename} not found. Checked: {[str(c) for c in candidates]}"
        )
    load_dotenv(env_file, override=True)
    logger.info("Loaded environment configuration from: %s", env_file.name)
    
    # Also load base .env if it exists (for shared variables)
    base_env_file = project_root / '.env'
    if base_env_file.exists():
        load_dotenv(base_env_file, override=False)


# Auto-load environment configuration when module is imported
# This ensures environment variables are available throughout the application
# Note: In Airflow, this is skipped as trading_agent_dags.py handles env loading
load_environment_config()
