"""
Filesystem locations: package lives under the repo root; config, duck_data, and assets stay at repo root.
"""

from pathlib import Path

PKG_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PKG_ROOT.parent

ASSETS_DIR = REPO_ROOT / "assets"
DUCK_DATA_DIR = REPO_ROOT / "duck_data"
HTML_DIR = DUCK_DATA_DIR / "html"
