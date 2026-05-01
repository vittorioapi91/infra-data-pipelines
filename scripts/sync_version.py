#!/usr/bin/env python3
"""
Sync src/__init__.py __version__ with the latest git tag.

Run after creating a new tag, or to fix drift. Reads the latest tag (e.g. v0.0.5),
strips the 'v' prefix, and updates src/__init__.py.

Usage:
    python scripts/sync_version.py
"""

import re
import subprocess
import sys
from pathlib import Path


def get_latest_tag(project_root: Path) -> str:
    """Return the latest git tag in the repo (e.g. v0.0.5), or raise if none."""
    result = subprocess.run(
        ["git", "tag", "-l", "--sort=-version:refname"],
        capture_output=True,
        text=True,
        check=False,
        cwd=project_root,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise SystemExit("No git tags found. Create a tag first (e.g. git tag v0.0.5).")
    tags = [t.strip() for t in result.stdout.strip().split("\n") if t.strip()]
    if not tags:
        raise SystemExit("No git tags found. Create a tag first (e.g. git tag v0.0.5).")
    return tags[0]


def update_init_version(project_root: Path, version: str) -> None:
    """Update __version__ in src/__init__.py."""
    init_path = project_root / "src" / "__init__.py"
    if not init_path.exists():
        raise SystemExit(f"Not found: {init_path}")
    content = init_path.read_text(encoding="utf-8")
    new_content = re.sub(
        r'__version__\s*=\s*["\'][^"\']*["\']',
        f'__version__ = "{version}"',
        content,
    )
    if new_content == content:
        raise SystemExit(f"Could not find __version__ in {init_path}")
    init_path.write_text(new_content, encoding="utf-8")
    print(f"Updated {init_path.relative_to(project_root)}: __version__ = \"{version}\"")


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent

    tag = get_latest_tag(project_root)
    version = tag.lstrip("v") if tag.startswith("v") else tag
    print(f"Latest tag: {tag} -> version {version}")

    update_init_version(project_root, version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
