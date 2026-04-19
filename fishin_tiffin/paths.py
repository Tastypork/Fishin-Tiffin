"""Filesystem locations used by the bot."""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

ASSETS_DIR = REPO_ROOT / "assets"

# Sibling checkout: Duck-Game-backend/duck_game_backend/static/game/* — the bot reads these
# files directly and uploads them as attachments so embeds work when the API is on a private host.
DUCK_GAME_STATIC_GAME_DIR = REPO_ROOT.parent / "Duck-Game-backend" / "duck_game_backend" / "static" / "game"
