
"""
Minimal cross-platform user data directory helper.

Usage:
    from userdirs import user_data_dir

    path = user_data_dir("aws-reporting")
    path.mkdir(parents=True, exist_ok=True)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def user_data_dir(app_name: str, roaming: bool = False) -> Path:
    """
    Return the per-user data directory for the given app name.

    Paths (examples):

    - Windows:
        roaming=False -> %LOCALAPPDATA%\\<AppName>
        roaming=True  -> %APPDATA%\\<AppName>

    - macOS:
        ~/Library/Application Support/<AppName>

    - Linux / other Unix:
        $XDG_DATA_HOME/<AppName> or ~/.local/share/<AppName>
    """
    if not app_name:
        raise ValueError("app_name must be a non-empty string")

    # Windows
    if sys.platform.startswith("win"):
        if roaming:
            base = os.getenv("APPDATA")
            # Fallback if APPDATA isn't defined for some reason
            if not base:
                base = Path.home() / "AppData" / "Roaming"
        else:
            base = os.getenv("LOCALAPPDATA") or os.getenv("APPDATA")
            if not base:
                base = Path.home() / "AppData" / "Local"

        return Path(base) / app_name

    # macOS
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / app_name

    # Linux / other Unix (XDG)
    xdg_data_home = os.getenv("XDG_DATA_HOME")
    if xdg_data_home:
        base = Path(xdg_data_home)
    else:
        base = Path.home() / ".local" / "share"

    return base / app_name


# Optional: tiny CLI using only the stdlib
def main() -> None:
    """
    Simple CLI:

        python -m userdirs aws-reporting
        python -m userdirs aws-reporting --roaming
    """
    import argparse

    parser = argparse.ArgumentParser(description="Print user data directory for an app.")
    parser.add_argument("app_name", help="Application name, e.g. 'aws-reporting'")
    parser.add_argument(
        "--roaming",
        action="store_true",
        help="Use roaming profile on Windows (APPDATA instead of LOCALAPPDATA).",
    )
    args = parser.parse_args()

    print(user_data_dir(args.app_name, roaming=args.roaming))


if __name__ == "__main__":
    main()
