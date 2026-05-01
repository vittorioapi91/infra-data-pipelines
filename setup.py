"""
Setup configuration for TradingPythonAgent

This setup.py builds wheels that are organized by environment in dist/{env}/ folders:
- dist/dev/trading_agent-{version}.whl for development
- dist/staging/trading_agent-{version}.whl for staging
- dist/prod/trading_agent-{version}.whl for production

Usage:
    # Build for dev environment
    ENV=dev python setup.py bdist_wheel

    # Build for staging environment
    ENV=staging python setup.py bdist_wheel

    # Build for prod environment
    ENV=prod python setup.py bdist_wheel
"""

import os
from pathlib import Path
from setuptools import setup, find_packages
from setuptools.command.build_py import build_py

# Import bdist_wheel from setuptools (preferred, required for setuptools >= v70.1)
# Fallback to wheel.bdist_wheel for older setuptools versions
try:
    from setuptools.command.bdist_wheel import bdist_wheel
except ImportError:
    # Fallback for older setuptools versions (< v70.1)
    try:
        from wheel.bdist_wheel import bdist_wheel
    except ImportError:
        # If neither is available, we can't build wheels - raise error
        raise ImportError("wheel package is required to build wheels. Install it with: pip install wheel")

from setuptools.command.egg_info import egg_info

# Read the README file for long description
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# Get environment from environment variable (default to 'dev')
env = os.getenv("ENV", "dev").lower()
if env not in ["dev", "staging", "prod"]:
    env = "dev"

# Read version from package __init__.py
version_file = Path(__file__).parent / "src" / "__init__.py"
version = "0.1.0"  # Default version
if version_file.exists():
    with open(version_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("__version__"):
                version = line.split("=")[1].strip().strip('"').strip("'")
                break

# Read requirements.txt for dependencies
requirements_file = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_file.exists():
    with open(requirements_file, "r", encoding="utf-8") as f:
        requirements = [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]

# Environment-specific requirements files
env_requirements_file = Path(__file__).parent / f"requirements-{env}.txt"
if env_requirements_file.exists():
    with open(env_requirements_file, "r", encoding="utf-8") as f:
        env_requirements = [
            line.strip()
            for line in f
            if line.strip()
            and not line.strip().startswith("#")
            and not line.strip().startswith("-r")
        ]
        # Extend base requirements with environment-specific ones
        requirements.extend(env_requirements)

# Use standard package name (environment is handled by folder structure in dist/)
package_name = "trading_agent"

# Put .egg-info under dist/{env}/ so editable installs match wheel layout (dist/dev, dist/staging, dist/prod)
_dist_dir = Path(__file__).resolve().parent / "dist" / env
_dist_dir.mkdir(parents=True, exist_ok=True)


class CustomEggInfo(egg_info):
    def initialize_options(self):
        super().initialize_options()
        self.egg_base = str(_dist_dir)


# Custom build command to include _airflow_dags_ directory as a package
# Note: find_packages should now find _airflow_dags_ since it uses underscores, but we keep this
# to ensure it's included even if find_packages behavior changes
class CustomBuildPy(build_py):
    def run(self):
        # Copy _airflow_dags_ directory manually to trading_agent/_airflow_dags_
        # Since package_dir maps trading_agent to src, we need to copy to trading_agent/ in build
        src_dir = Path(__file__).parent / "src" / "_airflow_dags_"
        if src_dir.exists():
            for file_path in src_dir.rglob("*"):
                if file_path.is_file():
                    # Get relative path from src/
                    rel_path = file_path.relative_to(Path(__file__).parent / "src")
                    # Copy to trading_agent/_airflow_dags_/ in build directory
                    dest = Path(self.build_lib) / "trading_agent" / rel_path
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    self.copy_file(str(file_path), str(dest))
        super().run()


# Custom bdist_wheel command to preserve underscore in package name
# (setuptools normalizes to hyphen in METADATA, but we want to keep underscore)
class CustomBdistWheel(bdist_wheel):
    def run(self):
        super().run()
        # Fix METADATA to use underscore instead of hyphen
        # Find the wheel file we just created
        import zipfile
        import tempfile
        import shutil
        
        wheel_dir = Path(self.dist_dir)
        # Search for wheel file - might have normalized name with hyphen
        wheels = list(wheel_dir.glob(f"{package_name}-{version}-*.whl"))
        # Also search for normalized name
        normalized_name = package_name.replace('_', '-')
        wheels.extend(list(wheel_dir.glob(f"{normalized_name}-{version}-*.whl")))
        
        for wheel_file in wheels:
            if not wheel_file.exists():
                continue
                
            # Read METADATA from wheel, fix it, and write back
            metadata_file = None
            metadata_content = None
            
            # First pass: find and read METADATA
            with zipfile.ZipFile(wheel_file, 'r') as zf_in:
                for item in zf_in.infolist():
                    if item.filename.endswith('/METADATA'):
                        metadata_file = item.filename
                        metadata_content = zf_in.read(item.filename).decode('utf-8')
                        break
            
            if metadata_file and metadata_content:
                # Replace Name: trading-agent with Name: trading_agent
                if 'Name: trading-agent' in metadata_content or f'Name: {normalized_name}' in metadata_content:
                    metadata_content = metadata_content.replace(
                        f'Name: {normalized_name}',
                        f'Name: {package_name}'
                    )
                    # Also replace if it somehow has trading-agent explicitly
                    metadata_content = metadata_content.replace(
                        'Name: trading-agent',
                        f'Name: {package_name}'
                    )
                    
                    # Write modified METADATA back to wheel
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.whl') as tmp_wheel:
                        with zipfile.ZipFile(wheel_file, 'r') as zf_in:
                            with zipfile.ZipFile(tmp_wheel.name, 'w', zipfile.ZIP_DEFLATED) as zf_out:
                                for item in zf_in.infolist():
                                    if item.filename == metadata_file:
                                        # Write modified METADATA
                                        zf_out.writestr(item, metadata_content.encode('utf-8'))
                                    else:
                                        # Copy other files as-is
                                        zf_out.writestr(item, zf_in.read(item.filename))
                        
                        # Replace original wheel with modified one
                        shutil.move(tmp_wheel.name, wheel_file)
                        break  # Only process first matching wheel

setup(
    name=package_name,
    version=version,
    description="TradingPythonAgent - A Python trading agent package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Vittorio Apicella",
    author_email="apicellavittorio@hotmail.it",
    url="https://github.com/vittorioapi91/TradingPythonAgent",
    # Map trading_agent.* packages to src/ directory
    # This creates trading_agent.fundamentals, trading_agent.macro, etc.
    # When importing trading_agent.fundamentals, Python will look in src/fundamentals/
    package_dir={"trading_agent": "src"},
    packages=[f"trading_agent.{pkg}" for pkg in find_packages(where="src") if pkg],  # Filter out empty strings
    python_requires=">=3.9",
    install_requires=requirements,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    # Include package data if needed
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml", "*.json", "*.sql"],
    },
    # Entry points for command-line scripts (if needed)
    entry_points={
        "console_scripts": [
            "trading-agent=src.agent:main",
        ],
    },
    # Custom build commands
    cmdclass={
        "build_py": CustomBuildPy,
        "bdist_wheel": CustomBdistWheel,
        "egg_info": CustomEggInfo,
    },
)
