#!/usr/bin/env python
"""Verify that the git tag matches the package version."""
import os
import re
import sys
from pathlib import Path


def main():
    # Read version from pyproject.toml
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"

    with open(pyproject_path, "r") as f:
        content = f.read()

    # Extract version using regex
    version_match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if not version_match:
        print("ERROR: Could not find version in pyproject.toml")
        sys.exit(1)

    package_version = version_match.group(1)

    # Get git tag from environment
    git_tag = os.getenv("CIRCLE_TAG")

    if git_tag != package_version:
        print(f"ERROR: Git tag '{git_tag}' does not match package version '{package_version}'")
        sys.exit(1)

    print(f"✓ Version verified: {package_version}")
    sys.exit(0)


if __name__ == "__main__":
    main()
